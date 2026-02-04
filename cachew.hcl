# Cachew unified configuration with tiered caching strategy
# Uses disk (L1) + S3 (L2) cache backends
#
# Required environment variable:
# - CACHEW_S3_BUCKET: S3 bucket name (REQUIRED)

# First tier: Disk cache (fast local access)
# 500GB limit hardcoded - uses defaults for everything else
disk {
  root     = "./state/cache"
  limit-mb = 512000  # 500GB
}

# Second tier: S3 cache (durable storage)
# Uses defaults for all optional fields (region=us-west-2, endpoint=s3.amazonaws.com, etc.)
s3 {
  bucket = "${CACHEW_S3_BUCKET}"
}

# Git strategy configuration
git {
  mirror-root     = "./state/git-mirrors"
  clone-depth     = 1000
  bundle-interval = "24h"
}

# GitHub releases caching
github-releases {
  token = "${GITHUB_TOKEN}"
}

# Go module proxy
gomod {
  proxy = "https://proxy.golang.org"
}
