# strategy git {}
# strategy docker {}
# strategy hermit {}
# strategy artifactory {
#   mitm = ["artifactory.square.com"]
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
