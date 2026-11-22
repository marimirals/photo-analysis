package api

import (
	"net/http"

	apispec "github.com/marimirals/photo-analysis/api"
)

// docsPage — Swagger UI. Сам интерфейс (JS и CSS) грузится с CDN: jsDelivr,
// а если он недоступен — unpkg. Описание API — наше, с /openapi.yaml.
const docsPage = `<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>Photo Analysis API</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css"
        onerror="this.onerror=null;this.href='https://unpkg.com/swagger-ui-dist@5/swagger-ui.css'">
  <style>body { margin: 0; } .topbar { display: none; }</style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script>
    function load(src, fallback) {
      var s = document.createElement('script');
      s.src = src;
      s.onload = start;
      s.onerror = function () {
        if (fallback) { load(fallback, null); return; }
        document.getElementById('swagger-ui').innerHTML =
          '<p style="font:16px sans-serif;padding:24px">Не удалось загрузить Swagger UI с CDN. ' +
          'Описание API можно открыть как файл: <a href="/openapi.yaml">/openapi.yaml</a></p>';
      };
      document.head.appendChild(s);
    }
    function start() {
      window.ui = SwaggerUIBundle({
        url: '/openapi.yaml',
        dom_id: '#swagger-ui',
        deepLinking: true,
        persistAuthorization: true,   // введённый X-User-ID не сбрасывается при обновлении страницы
        displayRequestDuration: true,
        tryItOutEnabled: true
      });
    }
    load('https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js',
         'https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js');
  </script>
</body>
</html>`

func serveDocs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(docsPage))
}

func serveOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	_, _ = w.Write(apispec.OpenAPI)
}
