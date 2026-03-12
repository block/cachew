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

# github-app {
#   app-id = "app-id-value"
#   private-key-path = "private-key-path-value"
#   installations = { "myorg" : "installation-id" }
# }

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

proxy { }
