# Artifactory caching proxy strategy
# strategy artifactory "example.jfrog.io" {
#   target = "https://example.jfrog.io"
# }

state = "./state"
url = "http://127.0.0.1:8080"
log {
  level = "debug"
}

opa {
  policy = <<EOF
  package cachew.authz
  default allow := false
  allow if startswith(input.remote_addr, "127.0.0.1:")
  allow if not input.path[0] in {"api", "admin"}
  EOF
}

# github-app {
#   app-id = "app-id-value"
#   private-key-path = "private-key-path-value"
#   installations = { "myorg" : "installation-id" }
# }

metrics {}

strategy git {
  #bundle-interval = "24h"
  snapshot-interval = "1h"
  repack-interval = "1h"
}

strategy host "https://ghcr.io" {
  headers = {
    "Authorization": "Bearer QQ=="
  }
}

strategy host "https://w3.org" {}

strategy github-releases {
  token = "${GITHUB_TOKEN}"
  private-orgs = ["alecthomas"]
}

strategy gomod {
  proxy = "https://proxy.golang.org"
}

strategy hermit { }

strategy proxy { }

cache disk {
  limit-mb = 250000
  max-ttl = "8h"
}
