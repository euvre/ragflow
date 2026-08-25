//
//  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"ragflow/internal/common"
	"ragflow/internal/entity"
)

// TestAgentUploadAndAttachmentDownload_ValidBetaToken drives the real
// BetaAuthMiddleware with a valid share (beta) token — the only credential
// the embedded agent page holds — into the real UploadAgentFile and
// DownloadAttachment handlers, and asserts the downstream handler
// response. The unauthenticated branch (middleware answers HTTP 200 +
// code 102) and the beta-vs-JWT route grouping are pinned separately in
// internal/router/agent_routes_test.go; what only this test can catch is a
// regression that makes the middleware reject valid beta tokens while the
// route table stays untouched.
//
// The resolver stub leaves GetUserByToken and GetUserByAPIToken unset, so
// both fail with their "not stubbed" defaults — the embedded page sends no
// JWT or API token, forcing the middleware to resolve through
// GetUserByBetaAPIToken.
func TestAgentUploadAndAttachmentDownload_ValidBetaToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	stub := &stubUserTokenResolver{
		getUserByBetaAPITokenFn: func(ctx context.Context, auth string) (*entity.User, common.ErrorCode, error) {
			if auth != "beta-share-token" {
				t.Errorf("GetUserByBetaAPIToken called with %q, want beta-share-token", auth)
			}
			return &entity.User{ID: "u-beta"}, common.CodeSuccess, nil
		},
		getAPITokenByBetaFn: func(ctx context.Context, auth string) (*entity.APIToken, error) {
			// No dialog binding: the share token is canvas-agnostic here.
			return &entity.APIToken{}, nil
		},
	}
	ah := &AuthHandler{userService: stub}
	h := &AgentHandler{
		loader: &fakeCanvasLoader{canvas: &entity.UserCanvas{ID: "canvas-1"}},
		fileService: &fakeAgentFileService{
			uploadList: []map[string]interface{}{{"id": "upload-1", "name": "f1.txt"}},
			blob:       []byte("ATTACHMENT-BYTES"),
		},
	}

	r := gin.New()
	g := r.Group("/api/v1/agents")
	g.Use(ah.BetaAuthMiddleware())
	g.POST("/:canvas_id/upload", h.UploadAgentFile)
	g.GET("/attachments/:attachment_id/download", h.DownloadAttachment)

	// (1) Upload with the share token: the request must clear the
	// middleware and run the full handler happy path (canvas access
	// check + upload descriptors), not stop at an auth envelope.
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	fw, err := mw.CreateFormFile("file", "f1.txt")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := fw.Write([]byte("f1.txt")); err != nil {
		t.Fatalf("write file: %v", err)
	}
	mw.Close()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/v1/agents/canvas-1/upload", body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "beta-share-token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("upload: status = %d, want 200; body = %s", w.Code, w.Body.String())
	}
	var uploaded struct {
		Code int                    `json:"code"`
		Data map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &uploaded); err != nil {
		t.Fatalf("upload: failed to decode response body: %v", err)
	}
	if uploaded.Code != 0 {
		t.Fatalf("upload: code = %d, want 0 (handler success envelope, not the middleware's 102); body = %s", uploaded.Code, w.Body.String())
	}
	if uploaded.Data["id"] != "upload-1" {
		t.Errorf("upload: data.id = %v, want upload-1; body = %s", uploaded.Data["id"], w.Body.String())
	}

	// (2) Download with the same share token: the handler streams the
	// blob with download disposition.
	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/agents/attachments/att-1/download", nil)
	req2.Header.Set("Authorization", "beta-share-token")
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("download: status = %d, want 200; body = %s", w2.Code, w2.Body.String())
	}
	if w2.Body.String() != "ATTACHMENT-BYTES" {
		t.Errorf("download: body = %q, want ATTACHMENT-BYTES", w2.Body.String())
	}
	if cd := w2.Header().Get("Content-Disposition"); !strings.Contains(cd, "attachment") {
		t.Errorf("download: Content-Disposition = %q, want attachment", cd)
	}

	// (3) Control: without the token the same route stops at the
	// middleware's code-102 envelope, proving (1) and (2) really passed
	// through BetaAuthMiddleware rather than bypassing it.
	req3 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/agents/attachments/att-1/download", nil)
	w3 := httptest.NewRecorder()
	r.ServeHTTP(w3, req3)

	var unauth struct {
		Code common.ErrorCode `json:"code"`
	}
	if err := json.Unmarshal(w3.Body.Bytes(), &unauth); err != nil {
		t.Fatalf("control: failed to decode response body: %v", err)
	}
	if w3.Code != http.StatusOK || unauth.Code != common.CodeDataError {
		t.Fatalf("control: status = %d code = %d, want HTTP 200 + code %d (middleware rejection envelope); body = %s", w3.Code, unauth.Code, common.CodeDataError, w3.Body.String())
	}
}
