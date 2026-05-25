package main

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

//go:embed web/build/*
var webAssets embed.FS

func ServeSPA(r *gin.Engine) {
	sub, err := fs.Sub(webAssets, "web/build")
	if err != nil {
		panic(err)
	}

	staticServer := http.FileServer(http.FS(sub))

	r.NoRoute(func(c *gin.Context) {
		path := c.Request.URL.Path

		// If it's an API route, let it fall through (Gin NoRoute will still handle it)
		if strings.HasPrefix(path, "/v1") {
			return
		}

		// Check if the file exists in the embedded FS
		f, err := sub.Open(strings.TrimPrefix(path, "/"))
		if err == nil {
			f.Close()
			staticServer.ServeHTTP(c.Writer, c.Request)
			return
		}

		// Otherwise, serve index.html for SPA routing
		c.Request.URL.Path = "/"
		staticServer.ServeHTTP(c.Writer, c.Request)
	})
}
