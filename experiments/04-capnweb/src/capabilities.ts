import type { Octokit } from "@octokit/rest";
import type { WebClient } from "@slack/web-api";

export interface ToolSessionApi {
  slack: WebClient;
  github: Octokit;
}
