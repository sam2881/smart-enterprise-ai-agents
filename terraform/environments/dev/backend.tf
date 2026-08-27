terraform {
  backend "gcs" {
    bucket = "agent-ai-test-461120-tfstate"
    prefix = "terraform/dev"
  }
}
