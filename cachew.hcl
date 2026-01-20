# strategy git {}
# strategy docker {}
# strategy hermit {}

# Artifactory caching proxy strategy
# artifactory "example.jfrog.io" {
#   target = "https://example.jfrog.io"
# }


git {
  mirror-root = "./state/git-mirrors"
  clone-depth = 1000
  bundle-interval = "24h"
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
