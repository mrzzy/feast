package feast

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// Returns a mocked google authentication provider
func mockGoogleProvider(token string) *GoogleProvider {
	return &GoogleProvider{
		// mock find default credentials implementation.
		findDefaultCredentials: func(ctx context.Context, scopes ...string) (*google.Credentials, error) {
			if scopes[0] != "openid" && scopes[1] != "email" {
				return nil, fmt.Errorf("Got bad scopes. Expected 'openid', 'email'")
			}

			return &google.Credentials{
				ProjectID: "",
				TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
					AccessToken: token,
				}),
			}, nil
		},
	}
}

// Create a mock OAuth HTTP server & Oauth Provider
type OAuthCredientialsRequest struct {
	GrantType    string `json:"grant_type"`
	ClientId     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Audience     string `json:"audience"`
}

func mockOAuthProvider(token string) (*httptest.Server, *OAuthProvider) {
	clientId := "id"
	clientSecret := "secret"
	audience := "http://localhost"
	path := "/oauth"

	// Create a mock OAuth server to test Oauth provider.
	handlers := http.NewServeMux()
	handlers.HandleFunc(path, func(resp http.ResponseWriter, req *http.Request) {
		reqBytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			fmt.Printf("Oauth server failed to read request: %v", err)
		}

		oauthReq := OAuthCredientialsRequest{}
		err = json.Unmarshal(reqBytes, &oauthReq)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			fmt.Printf("Oauth server failed to read request: %v", err)
		}

		if oauthReq.GrantType != "client_credentials" ||
			oauthReq.ClientId != clientId ||
			oauthReq.ClientSecret != clientSecret ||
			oauthReq.Audience != audience {

			resp.WriteHeader(http.StatusUnauthorized)
			fmt.Printf("Oauth server failed authenticate request")
		}

		_, err = resp.Write([]byte(fmt.Sprintf("{\"access_token\": \"%s\"}", token)))
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			fmt.Printf("Oauth server failed to write response: %v", err)
		}
	})

	srv := httptest.NewServer(handlers)
	url, _ := url.Parse(srv.URL + path)
	return srv, &OAuthProvider{
		ClientId:     "id",
		ClientSecret: "secret",
		Audience:     "http://localhost",
		EndpointURL:  url,
	}
}

func TestAuthProvider(t *testing.T) {
	srv, oauthProvider := mockOAuthProvider("oauth token")
	defer srv.Close()

	tt := []struct {
		name     string
		provider AuthProvider
		want     string
		wantErr  bool
		err      error
	}{
		{
			name: "Valid Static Authentication Provider get token.",
			provider: &StaticProvider{
				StaticToken: "static token",
			},
			want:    "static token",
			wantErr: false,
			err:     nil,
		},
		{
			name:     "Valid Google Authentication Provider get token.",
			provider: mockGoogleProvider("google token"),
			want:     "google token",
			wantErr:  false,
			err:      nil,
		},
		{
			name:     "Valid OAuth Authentication Provider get token.",
			provider: oauthProvider,
			want:     "oauth token",
			wantErr:  false,
			err:      nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			token, err := tc.provider.Token()
			if err != nil {
				t.Error(err)
			}

			if token != tc.want {
				t.Fatalf("Authentication provider returned '%s', expected '%s'", token, tc.want)
			}
		})
	}
}