/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proxy

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func parseURLOrDie(inURL string) *url.URL {
	parsed, err := url.Parse(inURL)
	if err != nil {
		panic(err)
	}
	return parsed
}

func TestProxyTransport(t *testing.T) {
	testTransport := &Transport{
		Scheme:      "http",
		Host:        "foo.com",
		PathPrepend: "/proxy/node/node1:10250",
	}
	testTransport2 := &Transport{
		Scheme:      "https",
		Host:        "foo.com",
		PathPrepend: "/proxy/node/node1:8080",
	}
	emptyHostTransport := &Transport{
		Scheme:      "https",
		PathPrepend: "/proxy/node/node1:10250",
	}
	emptySchemeTransport := &Transport{
		Host:        "foo.com",
		PathPrepend: "/proxy/node/node1:10250",
	}
	emptyHostAndSchemeTransport := &Transport{
		PathPrepend: "/proxy/node/node1:10250",
	}
	type Item struct {
		input        string
		sourceURL    string
		transport    *Transport
		output       string
		contentType  string
		forwardedURI string
		redirect     string
		redirectWant string
		reqHost      string
	}

	table := map[string]Item{
		"normal": {
			input:        `<meta content="/yyyyy" name="abc"></meta><pre><a href="kubelet.log">kubelet.log</a><a href="/google.log">google.log</a></pre>`,
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    testTransport,
			output:       `<meta content="http://foo.com/proxy/node/node1:10250/yyyyy" name="abc"><pre><a href="kubelet.log">kubelet.log</a><a href="http://foo.com/proxy/node/node1:10250/google.log">google.log</a></pre>`,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
		},
		"full document": {
			input:        `<html><header></header><body><pre><a href="kubelet.log">kubelet.log</a><a href="/google.log">google.log</a></pre></body></html>`,
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    testTransport,
			output:       `<html><header></header><body><pre><a href="kubelet.log">kubelet.log</a><a href="http://foo.com/proxy/node/node1:10250/google.log">google.log</a></pre></body></html>`,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
		},
		"trailing slash": {
			input:        `<pre><a href="kubelet.log">kubelet.log</a><a href="/google.log/">google.log</a></pre>`,
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    testTransport,
			output:       `<pre><a href="kubelet.log">kubelet.log</a><a href="http://foo.com/proxy/node/node1:10250/google.log/">google.log</a></pre>`,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
		},
		"content-type charset": {
			input:        `<pre><a href="kubelet.log">kubelet.log</a><a href="/google.log">google.log</a></pre>`,
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    testTransport,
			output:       `<pre><a href="kubelet.log">kubelet.log</a><a href="http://foo.com/proxy/node/node1:10250/google.log">google.log</a></pre>`,
			contentType:  "text/html; charset=utf-8",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
		},
		"content-type passthrough": {
			input:        `<pre><a href="kubelet.log">kubelet.log</a><a href="/google.log">google.log</a></pre>`,
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    testTransport,
			output:       `<pre><a href="kubelet.log">kubelet.log</a><a href="/google.log">google.log</a></pre>`,
			contentType:  "text/plain",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
		},
		"subdir": {
			input:        `<a href="kubelet.log">kubelet.log</a><a href="/google.log">google.log</a>`,
			sourceURL:    "http://mynode.com/whatever/apt/somelog.log",
			transport:    testTransport2,
			output:       `<a href="kubelet.log">kubelet.log</a><a href="https://foo.com/proxy/node/node1:8080/google.log">google.log</a>`,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:8080/whatever/apt/somelog.log",
		},
		"image": {
			input:        `<pre><img src="kubernetes.jpg"/><img src="/kubernetes_abs.jpg"/></pre>`,
			sourceURL:    "http://mynode.com/",
			transport:    testTransport,
			output:       `<pre><img src="kubernetes.jpg"/><img src="http://foo.com/proxy/node/node1:10250/kubernetes_abs.jpg"/></pre>`,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/",
		},
		"abs": {
			input:        `<script src="http://google.com/kubernetes.js"/>`,
			sourceURL:    "http://mynode.com/any/path/",
			transport:    testTransport,
			output:       `<script src="http://google.com/kubernetes.js"/>`,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/any/path/",
		},
		"abs but same host": {
			input:        `<script src="http://mynode.com/kubernetes.js"/>`,
			sourceURL:    "http://mynode.com/any/path/",
			transport:    testTransport,
			output:       `<script src="http://foo.com/proxy/node/node1:10250/kubernetes.js"/>`,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/any/path/",
		},
		"redirect rel": {
			sourceURL:    "http://mynode.com/redirect",
			transport:    testTransport,
			redirect:     "/redirected/target/",
			redirectWant: "http://foo.com/proxy/node/node1:10250/redirected/target/",
			forwardedURI: "/proxy/node/node1:10250/redirect",
		},
		"redirect abs same host": {
			sourceURL:    "http://mynode.com/redirect",
			transport:    testTransport,
			redirect:     "http://mynode.com/redirected/target/",
			redirectWant: "http://foo.com/proxy/node/node1:10250/redirected/target/",
			forwardedURI: "/proxy/node/node1:10250/redirect",
		},
		"redirect abs other host": {
			sourceURL:    "http://mynode.com/redirect",
			transport:    testTransport,
			redirect:     "http://example.com/redirected/target/",
			redirectWant: "http://example.com/redirected/target/",
			forwardedURI: "/proxy/node/node1:10250/redirect",
		},
		"redirect abs use reqHost no host no scheme": {
			sourceURL:    "http://mynode.com/redirect",
			transport:    emptyHostAndSchemeTransport,
			redirect:     "http://10.0.0.1:8001/redirected/target/",
			redirectWant: "http://10.0.0.1:8001/proxy/node/node1:10250/redirected/target/",
			forwardedURI: "/proxy/node/node1:10250/redirect",
			reqHost:      "10.0.0.1:8001",
		},
		"source contains the redirect already": {
			input:        `<pre><a href="kubelet.log">kubelet.log</a><a href="http://foo.com/proxy/node/node1:10250/google.log">google.log</a></pre>`,
			sourceURL:    "http://foo.com/logs/log.log",
			transport:    testTransport,
			output:       `<pre><a href="kubelet.log">kubelet.log</a><a href="http://foo.com/proxy/node/node1:10250/google.log">google.log</a></pre>`,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
		},
		"no host": {
			input:        "<html></html>",
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    emptyHostTransport,
			output:       "<html></html>",
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
		},
		"no scheme": {
			input:        "<html></html>",
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    emptySchemeTransport,
			output:       "<html></html>",
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
		},
		"forwarded URI must be escaped": {
			input:        "<html></html>",
			sourceURL:    "http://mynode.com/logs/log.log%00<script>alert(1)</script>",
			transport:    testTransport,
			output:       "<html></html>",
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log%00%3Cscript%3Ealert%281%29%3C/script%3E",
		},
		"redirect rel must be escaped": {
			sourceURL:    "http://mynode.com/redirect",
			transport:    testTransport,
			redirect:     "/redirected/target/%00<script>alert(1)</script>/",
			redirectWant: "http://foo.com/proxy/node/node1:10250/redirected/target/%00%3Cscript%3Ealert%281%29%3C/script%3E/",
			forwardedURI: "/proxy/node/node1:10250/redirect",
		},
		"redirect abs same host must be escaped": {
			sourceURL:    "http://mynode.com/redirect",
			transport:    testTransport,
			redirect:     "http://mynode.com/redirected/target/%00<script>alert(1)</script>/",
			redirectWant: "http://foo.com/proxy/node/node1:10250/redirected/target/%00%3Cscript%3Ealert%281%29%3C/script%3E/",
			forwardedURI: "/proxy/node/node1:10250/redirect",
		},
		"redirect abs other host must be escaped": {
			sourceURL:    "http://mynode.com/redirect",
			transport:    testTransport,
			redirect:     "http://example.com/redirected/target/%00<script>alert(1)</script>/",
			redirectWant: "http://example.com/redirected/target/%00%3Cscript%3Ealert%281%29%3C/script%3E/",
			forwardedURI: "/proxy/node/node1:10250/redirect",
		},
		"redirect abs use reqHost no host no scheme must be escaped": {
			sourceURL:    "http://mynode.com/redirect",
			transport:    emptyHostAndSchemeTransport,
			redirect:     "http://10.0.0.1:8001/redirected/target/%00<script>alert(1)</script>/",
			redirectWant: "http://10.0.0.1:8001/proxy/node/node1:10250/redirected/target/%00%3Cscript%3Ealert%281%29%3C/script%3E/",
			forwardedURI: "/proxy/node/node1:10250/redirect",
			reqHost:      "10.0.0.1:8001",
		},
		// 以下为 microhuang 追加的自定义 HTML 标签重写测试用例
		// =========================================================================
		"custom_static_rewrite": {
			input:        `<html><body><script src="http://old.com"></script></body></html>`,
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    testTransport,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
			rules: []UniversalTagRewriteRule{
				{
					TargetTagName:     "script",
					RewriteTargetAttr: "src",
					RewriteMode:       "static",
					RewriteConfig:     map[string]string{"targetValue": "https://example.com"},
				},
			},
			output: `<html><body><script src="https://example.com"></script></body></html>`, // 注意：如果官方 Item 里的输出字段叫 output，请把这一行改为 output: `...`
		},
		"custom_static_inject": {
			input:        `<html><body><img class="avatar"></body></html>`,
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    testTransport,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
			rules: []UniversalTagRewriteRule{
				{
					TargetTagName:     "img",
					RewriteTargetAttr: "src",
					RewriteMode:       "static",
					RewriteConfig:     map[string]string{"targetValue": "https://cdn.com"},
				},
			},
			output: `<html><body><img class="avatar" src="https://cdn.com"></body></html>`,
		},
		"custom_append_rewrite": {
			input:        `<html><head><meta name="version" content="v1.0"><meta name="fix" type="dynamic" content="abc"></head></html>`,
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    testTransport,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
			rules: []UniversalTagRewriteRule{
				{
					TargetTagName:     "meta",
					ExtraMatchAttrs: map[string]string{ // 属性过滤、多属性过滤支持
						"name": "fix",
						"type": "dynamic",
					},
					RewriteTargetAttr: "content",
					RewriteMode:       "append",
					RewriteConfig:     map[string]string{"appendSuffix": "-k8s-proxy-v1"},
				},
			},
			output: `<html><head><meta name="version" content="v1.0"><meta name="fix" type="dynamic" content="abc-k8s-proxy-v1"></head></html>`,
		},
		"custom_replace_rewrite": {
			input:        `<html><body><img src="/images/logo.png"></body></html>`,
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    testTransport,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
			rules: []UniversalTagRewriteRule{
				{
					TargetTagName:     "img",
					RewriteTargetAttr: "src",
					RewriteMode:       "replace",
					RewriteConfig:     map[string]string{"oldStr": "/images/", "newStr": "https://cdn.com"},
				},
			},
			output: `<html><body><img src="https://cdn.comlogo.png"></body></html>`,
		},
		"custom_replace_empty_defense": {
			input:        `<html><body><img src="/images/logo.png"></body></html>`,
			sourceURL:    "http://mynode.com/logs/log.log",
			transport:    testTransport,
			contentType:  "text/html",
			forwardedURI: "/proxy/node/node1:10250/logs/log.log",
			rules: []UniversalTagRewriteRule{
				{
					TargetTagName:     "img",
					RewriteTargetAttr: "src",
					RewriteMode:       "replace",
					RewriteConfig:     map[string]string{"oldStr": "", "newStr": "unsafe-hack"},
				},
			},
			expectedOutput: `<html><body><img src="/images/logo.png"></body></html>`, // 触发防御拦截，原样输出
		},
	}

	testItem := func(name string, item *Item) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Check request headers.
			if got, want := r.Header.Get("X-Forwarded-Uri"), item.forwardedURI; got != want {
				t.Errorf("%v: X-Forwarded-Uri = %q, want %q", name, got, want)
			}
			if len(item.transport.Host) == 0 {
				_, present := r.Header["X-Forwarded-Host"]
				if present {
					t.Errorf("%v: X-Forwarded-Host header should not be present", name)
				}
			} else {
				if got, want := r.Header.Get("X-Forwarded-Host"), item.transport.Host; got != want {
					t.Errorf("%v: X-Forwarded-Host = %q, want %q", name, got, want)
				}
			}
			if len(item.transport.Scheme) == 0 {
				_, present := r.Header["X-Forwarded-Proto"]
				if present {
					t.Errorf("%v: X-Forwarded-Proto header should not be present", name)
				}
			} else {
				if got, want := r.Header.Get("X-Forwarded-Proto"), item.transport.Scheme; got != want {
					t.Errorf("%v: X-Forwarded-Proto = %q, want %q", name, got, want)
				}
			}

			// Send response.
			if item.redirect != "" {
				http.Redirect(w, r, item.redirect, http.StatusMovedPermanently)
				return
			}
			w.Header().Set("Content-Type", item.contentType)
			fmt.Fprint(w, item.input)
		}))
		defer server.Close()

		// Replace source URL with our test server address.
		sourceURL := parseURLOrDie(item.sourceURL)
		serverURL := parseURLOrDie(server.URL)
		item.input = strings.Replace(item.input, sourceURL.Host, serverURL.Host, -1)
		item.redirect = strings.Replace(item.redirect, sourceURL.Host, serverURL.Host, -1)
		sourceURL.Host = serverURL.Host

		req, err := http.NewRequest(http.MethodGet, sourceURL.String(), nil)
		if err != nil {
			t.Errorf("%v: Unexpected error: %v", name, err)
			return
		}
		if item.reqHost != "" {
			req.Host = item.reqHost
		}
		resp, err := item.transport.RoundTrip(req)
		if err != nil {
			t.Errorf("%v: Unexpected error: %v", name, err)
			return
		}
		if item.redirect != "" {
			// Check that redirect URLs get rewritten properly.
			if got, want := resp.Header.Get("Location"), item.redirectWant; got != want {
				t.Errorf("%v: Location header = %q, want %q", name, got, want)
			}
			return
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("%v: Unexpected error: %v", name, err)
			return
		}
		if e, a := item.output, string(body); e != a {
			t.Errorf("%v: expected %v, but got %v", name, e, a)
		}
	}

	for name, item := range table {
		
		// TODO:microhuang: 核心传参通道。在进入测试前，判断当前用例的 transport 是否存在
		if item.transport != nil {
			currentRules := item.rules // 局部变量快照，防止闭包在多轮循环中被覆盖
			// 将当前单测项里配置的 rules 包装成 Provider 传递并注入到 Transport 实例中
			item.transport.ServiceTagRulesProvider = func() ([]UniversalTagRewriteRule, error) {
				return currentRules, nil
			}
		}
		
		testItem(name, &item)
	}
}

func TestRewriteResponse(t *testing.T) {
	gzipbuf := bytes.NewBuffer(nil)
	flatebuf := bytes.NewBuffer(nil)

	testTransport := &Transport{
		Scheme:      "http",
		Host:        "foo.com",
		PathPrepend: "/proxy/node/node1:10250",
	}
	expected := []string{
		"short body test",
		strings.Repeat("long body test", 4097),
	}
	test := []struct {
		encodeType string
		writer     func(string) *http.Response
		reader     func(*http.Response) string
	}{
		{
			encodeType: "gzip",
			writer: func(ept string) *http.Response {
				gzw := gzip.NewWriter(gzipbuf)
				defer gzw.Close()

				gzw.Write([]byte(ept))
				gzw.Flush()
				return &http.Response{
					Body: io.NopCloser(gzipbuf),
				}
			},
			reader: func(rep *http.Response) string {
				reader, _ := gzip.NewReader(rep.Body)
				s, _ := io.ReadAll(reader)
				return string(s)
			},
		},
		{
			encodeType: "deflate",
			writer: func(ept string) *http.Response {
				flw, _ := flate.NewWriter(flatebuf, flate.BestCompression)
				defer flw.Close()

				flw.Write([]byte(ept))
				flw.Flush()
				return &http.Response{
					Body: io.NopCloser(flatebuf),
				}
			},
			reader: func(rep *http.Response) string {
				reader := flate.NewReader(rep.Body)
				s, _ := io.ReadAll(reader)
				return string(s)
			},
		},
	}

	errFn := func(encode string, err error) {
		t.Errorf("%s failed to read and write: %v", encode, err)
	}
	for _, v := range test {
		request, _ := http.NewRequest(http.MethodGet, "http://mynode.com/", nil)
		request.Header.Set("Content-Encoding", v.encodeType)
		request.Header.Add("Accept-Encoding", v.encodeType)

		for _, exp := range expected {
			resp := v.writer(exp)
			gotResponse, err := testTransport.rewriteResponse(request, resp)

			if err != nil {
				errFn(v.encodeType, err)
			}

			result := v.reader(gotResponse)
			if result != exp {
				errFn(v.encodeType, fmt.Errorf("expected %s, get %s", exp, result))
			}
		}
	}
}
