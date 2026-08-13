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
	"regexp"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
)

// atomsToAttrs states which attributes of which tags require URL substitution.
// Sources: http://www.w3.org/TR/REC-html40/index/attributes.html
//
//	http://www.w3.org/html/wg/drafts/html/master/index.html#attributes-1
var atomsToAttrs = map[atom.Atom]sets.String{
	atom.A:          sets.NewString("href"),
	atom.Applet:     sets.NewString("codebase"),
	atom.Area:       sets.NewString("href"),
	atom.Audio:      sets.NewString("src"),
	atom.Base:       sets.NewString("href"),
	atom.Blockquote: sets.NewString("cite"),
	atom.Body:       sets.NewString("background"),
	atom.Button:     sets.NewString("formaction"),
	atom.Command:    sets.NewString("icon"),
	atom.Del:        sets.NewString("cite"),
	atom.Embed:      sets.NewString("src"),
	atom.Form:       sets.NewString("action"),
	atom.Frame:      sets.NewString("longdesc", "src"),
	atom.Head:       sets.NewString("profile"),
	atom.Html:       sets.NewString("manifest"),
	atom.Iframe:     sets.NewString("longdesc", "src"),
	atom.Img:        sets.NewString("longdesc", "src", "usemap"),
	atom.Input:      sets.NewString("src", "usemap", "formaction"),
	atom.Ins:        sets.NewString("cite"),
	atom.Link:       sets.NewString("href"),
	atom.Object:     sets.NewString("classid", "codebase", "data", "usemap"),
	atom.Q:          sets.NewString("cite"),
	atom.Script:     sets.NewString("src"),
	atom.Source:     sets.NewString("src"),
	atom.Video:      sets.NewString("poster", "src"),

	// TODO: microhuang
	//atom.Meta:       sets.NewString("content"),

	// TODO: css URLs hidden in style elements.
}

/*
apiVersion: v1
kind: Service
metadata:
  name: web-app-service
  annotations:
    # 核心配置项：自定义HTML标签重写规则列表
    proxy.config.k8s.io/custom-tag-rewrite-rules: |
      - targetTagName: "meta"
        # 【场景1】需要额外匹配：仅当 meta name="abc" 时生效
        extraMatchAttrs:
          name: "abc"
        rewriteTargetAttr: "content"
        rewriteMode: "append"
        rewriteConfig:
          appendSuffix: "-k8s-proxy-v1"
          
      - targetTagName: "img"
        # 【场景2】无需额外匹配：所有 img 标签均生效
        extraMatchAttrs: {}
        rewriteTargetAttr: "src"
        rewriteMode: "replace"
        rewriteConfig:
          oldStr: "/images/"
          newStr: "https://cdn.example.com/images/"
          
      - targetTagName: "script"
        # 【场景3】静态替换：将所有 script src 替换为指定CDN地址
        extraMatchAttrs: {}
        rewriteTargetAttr: "src"
        rewriteMode: "static"
        rewriteConfig:
          targetValue: "https://cdn.example.com/main.js"
*/
// TODO: microhuang
//var atomsToReg = map[atom.Atom]sets.String{
//       atom.Meta:       sets.NewString(" name=\"abc\""), //给meta标签需要转换的属性加上 来自外部的 额外的正则条件限定：<meta name="abc" ....>
//}

// TODO: microhuang,【新增通用重写规则结构体1】 完全适配Service YAML配置，支持任意HTML标签自定义重写
type UniversalTagRewriteRule struct {
	// 目标HTML标签名，如meta、img、a、script等
	TargetTagName string `json:"targetTagName" yaml:"targetTagName"`
	// 【可选】额外匹配属性：仅当标签同时命中这些属性键值对时，才执行重写逻辑
	// 留空时表示：只要命中标签名就直接执行重写，无需额外校验
	ExtraMatchAttrs map[string]string `json:"extraMatchAttrs" yaml:"extraMatchAttrs"`
	// 需要重写的目标属性名，如meta的content、img的src、a的href
	RewriteTargetAttr string `json:"rewriteTargetAttr" yaml:"rewriteTargetAttr"`
	// 重写模式：static(静态替换)、append(追加内容)、replace(替换指定子串)
	RewriteMode string `json:"rewriteMode" yaml:"rewriteMode"`
	// 重写参数配置，不同模式传入对应所需值
	RewriteConfig map[string]string `json:"rewriteConfig" yaml:"rewriteConfig"`
}

// Transport is a transport for text/html content that replaces URLs in html
// content with the prefix of the proxy server
type Transport struct {
	Scheme      string
	Host        string
	PathPrepend string

	http.RoundTripper

	// TODO: microhuang,【新增配置注入字段2】 当前代理实例绑定的规则加载器，从对应Service YAML注解中读取所有自定义重写规则
	ServiceTagRulesProvider func() ([]UniversalTagRewriteRule, error)
}

// RoundTrip implements the http.RoundTripper interface
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Add reverse proxy headers.
	forwardedURI := path.Join(t.PathPrepend, req.URL.EscapedPath())
	if strings.HasSuffix(req.URL.Path, "/") {
		forwardedURI = forwardedURI + "/"
	}
	req.Header.Set("X-Forwarded-Uri", forwardedURI)
	if len(t.Host) > 0 {
		req.Header.Set("X-Forwarded-Host", t.Host)
	}
	if len(t.Scheme) > 0 {
		req.Header.Set("X-Forwarded-Proto", t.Scheme)
	}

	rt := t.RoundTripper
	if rt == nil {
		rt = http.DefaultTransport
	}
	resp, err := rt.RoundTrip(req)

	if err != nil {
		return nil, errors.NewServiceUnavailable(fmt.Sprintf("error trying to reach service: %v", err))
	}

	if redirect := resp.Header.Get("Location"); redirect != "" {
		targetURL, err := url.Parse(redirect)
		if err != nil {
			return nil, errors.NewInternalError(fmt.Errorf("error trying to parse Location header: %v", err))
		}
		resp.Header.Set("Location", t.rewriteURL(targetURL, req.URL, req.Host))
		return resp, nil
	}

	cType := resp.Header.Get("Content-Type")
	cType = strings.TrimSpace(strings.SplitN(cType, ";", 2)[0])
	if cType != "text/html" {
		// Do nothing, simply pass through
		return resp, nil
	}

	return t.rewriteResponse(req, resp)
}

var _ = net.RoundTripperWrapper(&Transport{})

func (rt *Transport) WrappedRoundTripper() http.RoundTripper {
	return rt.RoundTripper
}

// rewriteURL rewrites a single URL to go through the proxy, if the URL refers
// to the same host as sourceURL, which is the page on which the target URL
// occurred, or if the URL matches the sourceRequestHost.
func (t *Transport) rewriteURL(url *url.URL, sourceURL *url.URL, sourceRequestHost string) string {
	// Example:
	//      When API server processes a proxy request to a service (e.g. /api/v1/namespace/foo/service/bar/proxy/),
	//      the sourceURL.Host (i.e. req.URL.Host) is the endpoint IP address of the service. The
	//      sourceRequestHost (i.e. req.Host) is the Host header that specifies the host on which the
	//      URL is sought, which can be different from sourceURL.Host. For example, if user sends the
	//      request through "kubectl proxy" locally (i.e. localhost:8001/api/v1/namespace/foo/service/bar/proxy/),
	//      sourceRequestHost is "localhost:8001".
	//
	//      If the service's response URL contains non-empty host, and url.Host is equal to either sourceURL.Host
	//      or sourceRequestHost, we should not consider the returned URL to be a completely different host.
	//      It's the API server's responsibility to rewrite a same-host-and-absolute-path URL and append the
	//      necessary URL prefix (i.e. /api/v1/namespace/foo/service/bar/proxy/).
	isDifferentHost := url.Host != "" && url.Host != sourceURL.Host && url.Host != sourceRequestHost
	isRelative := !strings.HasPrefix(url.Path, "/")
	if isDifferentHost || isRelative {
		return url.String()
	}

	// Do not rewrite scheme and host if the Transport has empty scheme and host
	// when targetURL already contains the sourceRequestHost
	if !(url.Host == sourceRequestHost && t.Scheme == "" && t.Host == "") {
		url.Scheme = t.Scheme
		url.Host = t.Host
	}

	origPath := url.Path
	// Do not rewrite URL if the sourceURL already contains the necessary prefix.
	if strings.HasPrefix(url.Path, t.PathPrepend) {
		return url.String()
	}
	url.Path = path.Join(t.PathPrepend, url.Path)
	if strings.HasSuffix(origPath, "/") {
		// Add back the trailing slash, which was stripped by path.Join().
		url.Path += "/"
	}

	return url.String()
}

// rewriteHTML scans the HTML for tags with url-valued attributes, and updates
// those values with the urlRewriter function. The updated HTML is output to the
// writer.

// TODO: microhuang【扩展升级】新增通用重写规则入参，执行自定义标签重写逻辑
func rewriteHTML(reader io.Reader, writer io.Writer, urlRewriter func(*url.URL) string, customRules []UniversalTagRewriteRule) error {
// func rewriteHTML(reader io.Reader, writer io.Writer, urlRewriter func(*url.URL) string) error {
	// Note: This assumes the content is UTF-8.
	tokenizer := html.NewTokenizer(reader)

	var err error
	for err == nil {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			err = tokenizer.Err()
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			if urlAttrs, ok := atomsToAttrs[token.DataAtom]; ok {
                               //var matched = true //
                               //if re, ok := atomsToReg[token.DataAtom].PopAny(); ok { //标签存在额外的正则匹配要求
                               //        rex := regexp.MustCompile(re)
                               //        matched = rex.MatchString(token.String()) //且符合正则条件时
                               //}
                               //if matched {
	
				for i, attr := range token.Attr {
					if urlAttrs.Has(attr.Key) {
						url, err := url.Parse(attr.Val)
						if err != nil {
							// Do not rewrite the URL if it isn't valid.  It is intended not
							// to error here to prevent the inability to understand the
							// content of the body to cause a fatal error.
							continue
						}
						token.Attr[i].Val = urlRewriter(url)
					}
				}

								//}
			}

			// TODO: microhuang,【新增通用重写引擎3】 匹配所有Service配置的自定义标签重写规则
			for _, rule := range customRules {
				// 第一步：快速匹配目标标签名
				if token.Data != rule.TargetTagName {
					continue
				}

				// 第二步：校验额外匹配属性，规则未配置ExtraMatchAttrs则直接跳过校验
				matchPassed := true
				for matchKey, matchVal := range rule.ExtraMatchAttrs {
					attrExist := false
					attrActualVal := ""
					for _, attr := range token.Attr {
						if attr.Key == matchKey {
							attrExist = true
							attrActualVal = attr.Val
							break
						}
					}
					// 属性不存在或值不匹配，直接中断当前规则匹配
					if !attrExist || attrActualVal != matchVal {
						matchPassed = false
						break
					}
				}
				if !matchPassed {
					continue
				}

				// 第三步：定位需要重写的目标属性索引
				targetAttrIdx := -1
				for i, attr := range token.Attr {
					if attr.Key == rule.RewriteTargetAttr {
						targetAttrIdx = i
						break
					}
				}
				// 目标属性不存在直接跳过
				/*if targetAttrIdx == -1 {
					klog.V(5).Infof("skip rewrite rule for tag %s, target attr %s not found", rule.TargetTagName, rule.RewriteTargetAttr)
					continue
				}*/

				// 第四步：执行重写逻辑
				var originVal string
				if targetAttrIdx != -1 {
					originVal = token.Attr[targetAttrIdx].Val
				}
				//originVal := token.Attr[targetAttrIdx].Val
				var newVal string
				skipCurrentRule := false
				switch rule.RewriteMode {
				case "static":
					newVal = rule.RewriteConfig["targetValue"]
				case "append":
					if targetAttrIdx == -1 {
						skipCurrentRule = true // 属性都不存在，无法追加，安全跳过
						break
					}
					newVal = originVal + rule.RewriteConfig["appendSuffix"]
				case "replace":
					if targetAttrIdx == -1 {
						skipCurrentRule = true // 属性都不存在，无法替换，安全跳过
						break
					}
					oldStr := rule.RewriteConfig["oldStr"]
					newStr := rule.RewriteConfig["newStr"]
					if oldStr == "" {
						skipCurrentRule = true
						break
					}
					newVal = strings.ReplaceAll(originVal, oldStr, newStr)
				default:
					newVal = originVal
				}
				if skipCurrentRule {
					continue
				}
				// if rule.NeedProxyRewrite && newVal != "" // 默认加上proxy前缀
				{
					parsedURL, err := url.Parse(newVal)
					if err == nil {
						// 调用官方传入的 urlRewriter，强制给 newVal 加上标准的 proxy 前缀地址
						newVal = urlRewriter(parsedURL)
					} else {
						klog.V(4).Infof("custom tag rewrite url parse failed: %v", err)
					}
				}
				//token.Attr[targetAttrIdx].Val = newVal
				if targetAttrIdx != -1 {
					token.Attr[targetAttrIdx].Val = newVal
				} else {
					token.Attr = append(token.Attr, html.Attribute{Key: rule.RewriteTargetAttr, Val: newVal})
				}
				klog.V(4).Infof("executed custom tag rewrite: tag=%s, originVal=%q, newVal=%q", rule.TargetTagName, originVal, newVal)
			}
			
			_, err = writer.Write([]byte(token.String()))
		default:
			_, err = writer.Write(tokenizer.Raw())
		}
	}
	if err != io.EOF {
		return err
	}
	return nil
}

// rewriteResponse modifies an HTML response by updating absolute links referring
// to the original host to instead refer to the proxy transport.
func (t *Transport) rewriteResponse(req *http.Request, resp *http.Response) (*http.Response, error) {
	origBody := resp.Body
	defer origBody.Close()

	newContent := &bytes.Buffer{}
	var reader io.Reader = origBody
	var writer io.Writer = newContent

	// TODO:microhuang，定义一个闭包用于最后阶段的手动 Flush/Close，解决安全闭合问题
	var closeWriter func() error
	
	encoding := resp.Header.Get("Content-Encoding")
	switch encoding {
	case "gzip":
		var err error
		reader, err = gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("errorf making gzip reader: %v", err)
		}
		gzw := gzip.NewWriter(writer)
		defer gzw.Close()
		writer = gzw

		// TODO:microhuang，赋值闭包：手动触发 Close 才能确保尾部数据（Trailer）落盘
		closeWriter = func() error {
			return gzw.Close()
		}
	case "deflate":
		var err error
		reader = flate.NewReader(reader)
		flw, err := flate.NewWriter(writer, flate.BestCompression)
		if err != nil {
			return nil, fmt.Errorf("errorf making flate writer: %v", err)
		}
		defer func() {
			flw.Close()
			flw.Flush()
		}()
		writer = flw

		// TODO:microhuang，赋值闭包：deflate 必须先 Flush 再 Close
		closeWriter = func() error {
			if err := flw.Flush(); err != nil {
				return err
			}
			return flw.Close()
		}
	case "":
		// This is fine

		// TODO:microhuang，明文传输不需要额外的压缩清理逻辑
		closeWriter = func() error { return nil }
	default:
		// Some encoding we don't understand-- don't try to parse this
		klog.Errorf("Proxy encountered encoding %v for text/html; can't understand this so not fixing links.", encoding)
		return resp, nil
	}

	// TODO:microhuang, 【新增规则拉取逻辑4】 拉取当前请求对应Service的所有自定义重写规则，异常场景自动降级返回空规则不中断代理
	var customRewriteRules []UniversalTagRewriteRule
	if t.ServiceTagRulesProvider != nil {
		var err error
		customRewriteRules, err = t.ServiceTagRulesProvider()
		if err != nil {
			klog.FromContext(req.Context()).V(4).Infof("failed to load custom rewrite rules from service yaml, skip all custom tag rewrite: %v", err)
			customRewriteRules = []UniversalTagRewriteRule{}
		}
	}

	urlRewriter := func(targetUrl *url.URL) string {
		return t.rewriteURL(targetUrl, req.URL, req.Host)
	}

	// TODO:microhuang, 传入自定义规则，执行完整HTML处理逻辑
	err := rewriteHTML(reader, writer, urlRewriter, customRewriteRules)
	// err := rewriteHTML(reader, writer, urlRewriter)
	if err != nil {
		klog.FromContext(req.Context()).Error(err, "Failed to rewrite URLs and custom tags")
		// klog.Errorf("Failed to rewrite URLs: %v", err)
		return resp, err
	}
	// TODO:microhuang, 【核心修复点】在计算长度前，如果存在压缩器，必须强制其闭合排空缓冲区
	if closeWriter != nil {
		if err := closeWriter(); err != nil {
			return nil, fmt.Errorf("error closing compression writer: %v", err)
		}
	}

	resp.Body = io.NopCloser(newContent)
	// Update header node with new content-length
	// TODO: Remove any hash/signature headers here?
	resp.Header.Del("Content-Length")
	resp.ContentLength = int64(newContent.Len())

	return resp, err
}
