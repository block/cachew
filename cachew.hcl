# strategy git {}
# strategy docker {}
# strategy hermit {}

# Artifactory caching proxy strategy
# artifactory "example.jfrog.io" {
#   target = "https://example.jfrog.io"
# }


git {
  mirror-root = "./state/git-mirrors"
}

host "https://w3.org" {}

github-releases {
  token = "${GITHUB_TOKEN}"
  private-orgs = ["alecthomas"]
}

memory {}

disk {
  root = "./state/cache"
}
