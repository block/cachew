git-credential-command "image-test" {
  command = ["/app/bin/git-credential-command"]
  remotes = ["${CACHEW_TEST_GIT_REMOTE}"]
  timeout = "5s"
  refresh-before = "5m"
}
