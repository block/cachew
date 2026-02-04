# Artifactory caching proxy strategy
# artifactory "example.jfrog.io" {
#   target = "https://example.jfrog.io"
# }

url = "http://127.0.0.1:8080"
log {
  level = "debug"
}

git-clone {
    mirror-root = "./state/git-mirrors"
}

github-app {
  app-id             = "${GITHUB_APP_ID}"
  private-key-path   = "${GITHUB_APP_PRIVATE_KEY_PATH}"
  installations-json = "${GITHUB_APP_INSTALLATIONS}"
}

metrics {}


git {
  bundle-interval = "24h"
  snapshot-interval = "24h"
}

host "https://w3.org" {}

github-releases {
  token = "${GITHUB_TOKEN}"
  private-orgs = ["alecthomas"]
}

disk {
  root = "./state/cache"
  limit-mb = 250000
  max-ttl = "8h"
}

gomod {
  proxy = "https://proxy.golang.org"
}

hermit { }
