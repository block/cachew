# Artifactory caching proxy strategy
# artifactory "example.jfrog.io" {
#   target = "https://example.jfrog.io"
# }

state = "./state"
url = "http://127.0.0.1:8080"
log {
  level = "debug"
}

git-clone {}

github-app {
  # Uncomment and add:
  # app-id = "app-id-value" (Can also be passed via setting envar CACHEW_GITHUB_APP_APP_ID)
  # private-key-path = "private-key-path-value" (Can also be passed via envar CACHEW_GITHUB_APP_PRIVATE_KEY_PATH)
  # installations = { "myorg" : "installation-id" }
}

metrics {}


git {
  #bundle-interval = "24h"
  snapshot-interval = "1h"
  repack-interval = "1h"
}

host "https://w3.org" {}

github-releases {
  token = "${GITHUB_TOKEN}"
  private-orgs = ["alecthomas"]
}

disk {
  limit-mb = 250000
  max-ttl = "8h"
}

gomod {
  proxy = "https://proxy.golang.org"
}

hermit { }
