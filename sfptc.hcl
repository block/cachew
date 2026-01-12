# strategy git {}
# strategy docker {}
# strategy hermit {}
# strategy artifactory {
#   mitm = ["artifactory.square.com"]
# }

host "https://w3.org" {}

github-releases {
  token = "${GITHUB_TOKEN}"
  private-orgs = ["alecthomas"]
}

memory {}

disk {
  root = "./cache"
}
