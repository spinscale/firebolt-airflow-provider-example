INSERT INTO gharchive
SELECT
  JSON_EXTRACT(src, '/id', 'TEXT') as event_id,
  JSON_EXTRACT(src, '/type', 'TEXT') as event_type,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/created_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/created_at', 'TEXT'), 12, 8))::timestamp as created_at,

  -- actor
  JSON_EXTRACT(src, '/actor/id', 'INT')::bigint as actor_id,
  JSON_EXTRACT(src, '/actor/login', 'TEXT') as actor_login,
  -- required as display_login did not exist before 2016-05
  COALESCE(JSON_EXTRACT(src, '/actor/display_login', 'TEXT'), JSON_EXTRACT(src, '/actor/login', 'TEXT')) as actor_display_login,
  JSON_EXTRACT(src, '/actor/gravatar_id', 'TEXT') as actor_gravatar_id,
  JSON_EXTRACT(src, '/actor/avatar_url', 'TEXT') as actor_avatar_url,

  -- repo
  JSON_EXTRACT(src, '/repo/id', 'int')::bigint as repo_id,
  JSON_EXTRACT(src, '/repo/name', 'TEXT') as repo_name,

  -- org
  JSON_EXTRACT(src, '/org/id', 'INT')::bigint as org_id,
  JSON_EXTRACT(src, '/org/login', 'TEXT') as org_login,
  JSON_EXTRACT(src, '/org/gravatar_id', 'TEXT') as org_gravatar_id,
  JSON_EXTRACT(src, '/org/avatar_url', 'TEXT') as org_avatar_url,

  -- shared fields among several events
  JSON_EXTRACT(src, '/payload/action', 'TEXT') as action,
  -- html_url
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestEvent'
      THEN JSON_EXTRACT(src, '/payload/html_url', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssueCommentEvent'
      THEN JSON_EXTRACT(src, '/payload/issue/html_url', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestReviewEvent'
      THEN JSON_EXTRACT(src, '/payload/pull_request/html_url', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssuesEvent'
      THEN JSON_EXTRACT(src, '/payload/issue/html_url', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestReviewCommentEvent'
      THEN JSON_EXTRACT(src, '/payload/comment/html_url', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'CommitCommentEvent'
      THEN JSON_EXTRACT(src, '/payload/comment/html_url', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'ReleaseEvent'
      THEN JSON_EXTRACT(src, '/payload/release/html_url', 'TEXT')
    ELSE
      NULL
  END as html_url,
  -- labels
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestEvent'
      THEN TRANSFORM(x -> JSON_EXTRACT(x, '/name','String'), JSON_EXTRACT_ARRAY_RAW(src, '/payload/labels'))
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssueComment'
      THEN TRANSFORM(x -> JSON_EXTRACT(x, '/name','String'), JSON_EXTRACT_ARRAY_RAW(src, '/payload/issue/labels'))
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssuesEvent'
      THEN TRANSFORM(x -> JSON_EXTRACT(x, '/name','String'), JSON_EXTRACT_ARRAY_RAW(src, '/payload/issue/labels'))
    ELSE
      []
  END as labels,
  -- assignees
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestEvent'
      THEN TRANSFORM(x -> JSON_EXTRACT(x, '/login', 'String'), JSON_EXTRACT_ARRAY_RAW(src, '/payload/assignees'))
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestReviwEvent'
      THEN TRANSFORM(x -> JSON_EXTRACT(x, '/login', 'String'), JSON_EXTRACT_ARRAY_RAW(src, '/payload/pull_request/assignees'))
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssuesEvent'
      THEN TRANSFORM(x -> JSON_EXTRACT(x, '/login', 'String'), JSON_EXTRACT_ARRAY_RAW(src, '/payload/issue/assignees'))
    ELSE
      []
  END as assignees,
  -- title
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestEvent'
      THEN JSON_EXTRACT(src, '/payload/title', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssueComment'
      THEN JSON_EXTRACT(src, '/payload/issue/title', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestReviewEvent'
      THEN JSON_EXTRACT(src, '/payload/pull_request/title', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssuesEvent'
      THEN JSON_EXTRACT(src, '/payload/issue/title', 'TEXT')
    ELSE
      NULL
  END as title,
  --body,
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestEvent'
      THEN JSON_EXTRACT(src, '/payload/body', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssueComment'
      THEN JSON_EXTRACT(src, '/payload/comment/body', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssuesEvent'
      THEN JSON_EXTRACT(src, '/payload/issue/body', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestReviewEvent'
      THEN JSON_EXTRACT(src, '/payload/comment/body', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'CommitCommentEvent'
      THEN JSON_EXTRACT(src, '/payload/comment/body', 'TEXT')
    ELSE
      NULL
  END as body,
  -- milestone
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestEvent'
      THEN JSON_EXTRACT(src, '/payload/milestone', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssuesEvent'
      THEN JSON_EXTRACT(src, '/payload/issue/milestone', 'TEXT')
    ELSE
      NULL
  END as milestone,
  -- draft,
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestEvent'
      THEN JSON_EXTRACT(src, '/payload/draft', 'TEXT')::boolean
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'ReleaseEvent'
      THEN JSON_EXTRACT(src, '/payload/release/draft', 'TEXT')::boolean
    ELSE
      NULL
  END as draft,
  -- payload_user_login
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestEvent'
      THEN JSON_EXTRACT(src, '/payload/user/login', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssueEvent'
      THEN JSON_EXTRACT(src, '/payload/issue/user/login', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssueCommentEvent'
      THEN JSON_EXTRACT(src, '/payload/comment/user/login', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestReviewEvent'
      THEN JSON_EXTRACT(src, '/payload/review/user/login', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssuesEvent'
      THEN JSON_EXTRACT(src, '/payload/issue/user/login', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'ForkEvent'
      THEN JSON_EXTRACT(src, '/payload/forkee/owner/login', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestReviewCommentEvent'
      THEN JSON_EXTRACT(src, '/payload/comment/user/login', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'CommitComment'
      THEN JSON_EXTRACT(src, '/payload/comment/user/login', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'ReleaseEvent'
      THEN JSON_EXTRACT(src, '/payload/release/author/login', 'TEXT')
    ELSE
      NULL
  END as payload_user_login,
  --payload_user_type
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestEvent'
      THEN JSON_EXTRACT(src, '/payload/user/type', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssueEvent'
      THEN JSON_EXTRACT(src, '/payload/issue/user/type', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssueCommentEvent'
      THEN JSON_EXTRACT(src, '/payload/comment/user/type', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestReviewEvent'
      THEN JSON_EXTRACT(src, '/payload/review/user/type', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssuesEvent'
      THEN JSON_EXTRACT(src, '/payload/issue/user/type', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'ForkEvent'
      THEN JSON_EXTRACT(src, '/payload/forkee/owner/type', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestReviewCommentEvent'
      THEN JSON_EXTRACT(src, '/payload/comment/user/type', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'CommitComment'
      THEN JSON_EXTRACT(src, '/payload/comment/user/type', 'TEXT')
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'ReleaseEvent'
      THEN JSON_EXTRACT(src, '/payload/release/author/type', 'TEXT')
    ELSE
      NULL
  END as payload_user_type,
  -- payload_user_id
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestEvent'
      THEN JSON_EXTRACT(src, '/payload/user/id', 'INT')::bigint
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssueEvent'
      THEN JSON_EXTRACT(src, '/payload/issue/user/id', 'INT')::bigint
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssueCommentEvent'
      THEN JSON_EXTRACT(src, '/payload/comment/user/id', 'INT')::bigint
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestReviewEvent'
      THEN JSON_EXTRACT(src, '/payload/review/user/id', 'INT')::bigint
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'IssuesEvent'
      THEN JSON_EXTRACT(src, '/payload/issue/user/id', 'INT')::bigint
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'ForkEvent'
      THEN JSON_EXTRACT(src, '/payload/forkee/owner/id', 'INT')::bigint
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PullRequestReviewCommentEvent'
      THEN JSON_EXTRACT(src, '/payload/comment/user/id', 'INT')::bigint
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'CommitComment'
      THEN JSON_EXTRACT(src, '/payload/comment/user/id', 'INT')::bigint
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'ReleaseEvent'
      THEN JSON_EXTRACT(src, '/payload/release/author/id', 'INT')::bigint
    ELSE
      NULL
  END as payload_user_id,
 
  -- push event
  JSON_EXTRACT(src, '/payload/push_id', 'INT')::bigint as push_event_push_id,
  JSON_EXTRACT(src, '/payload/size', 'INT') as push_event_size,
  JSON_EXTRACT(src, '/payload/distinct_size', 'INT') as push_event_distinct_size,
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'PushEvent'
      THEN JSON_EXTRACT(src, '/payload/ref', 'TEXT')
    ELSE
      NULL
  END as push_event_ref,
  JSON_EXTRACT(src, '/payload/head', 'TEXT') as push_event_head,
  JSON_EXTRACT(src, '/payload/before', 'TEXT') as push_event_before,
  TRANSFORM(
        x -> JSON_EXTRACT(x,'/sha','TEXT'),
        JSON_EXTRACT_ARRAY_RAW(src, '/payload/commits')
  ) as push_event_commits_shas,
  TRANSFORM(
        x -> JSON_EXTRACT(x,'/author/email','TEXT'),
        JSON_EXTRACT_ARRAY_RAW(src, '/payload/commits')
  ) as push_event_commits_author_email,
  TRANSFORM(
        x -> JSON_EXTRACT(x,'/author/name','TEXT'),
        JSON_EXTRACT_ARRAY_RAW(src, '/payload/commits')
  ) as push_event_commits_author_name,
    TRANSFORM(
        x -> JSON_EXTRACT(x,'/message','String'),
        JSON_EXTRACT_ARRAY_RAW(src, '/payload/commits')
  ) as push_event_commits_message,

  -- create event
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'CreateEvent'
      THEN JSON_EXTRACT(src, '/payload/ref', 'TEXT')
    ELSE
      NULL
  END as create_event_ref,
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'CreateEvent'
      THEN JSON_EXTRACT(src, '/payload/ref_type', 'TEXT')
    ELSE
      NULL
  END as create_event_ref_type,
  JSON_EXTRACT(src, '/payload/master_branch', 'TEXT') as create_event_master_branch,
  JSON_EXTRACT(src, '/payload/description', 'TEXT') as create_event_description,
  JSON_EXTRACT(src, '/payload/pusher_type', 'TEXT') as create_event_pusher_type,

  -- pull request event
  JSON_EXTRACT(src, '/payload/number', 'INT') as pull_request_event_number,
  JSON_EXTRACT(src, '/payload/id', 'INT')::bigint as pull_request_event_id,
  JSON_EXTRACT(src, '/payload/diff_url', 'TEXT') as pull_request_event_diff_url,
  JSON_EXTRACT(src, '/payload/patch_url', 'TEXT') as pull_request_event_patch_url,
  JSON_EXTRACT(src, '/payload/state', 'TEXT') as pull_request_event_state,
  JSON_EXTRACT(src, '/payload/locked', 'TEXT')::boolean as pull_request_event_locked,
  JSON_EXTRACT(src, '/payload/user/site_admin', 'TEXT')::boolean as pull_request_event_user_site_admin,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/created_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/created_at', 'TEXT'), 12, 8))::timestamp as pull_request_event_created_at,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/updated_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/updated_at', 'TEXT'), 12, 8))::timestamp as pull_request_event_updated_at,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/merged_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/merged_at', 'TEXT'), 12, 8))::timestamp as pull_request_event_merged_at,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/closed_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/closed_at', 'TEXT'), 12, 8))::timestamp as pull_request_event_closed_at,
  TRANSFORM(
        x -> JSON_EXTRACT(x,'/login','String'),
        JSON_EXTRACT_ARRAY_RAW(src, '/payload/requested_reviewers')
  ) as pull_request_event_requested_reviewers_login,
  JSON_EXTRACT(src, '/payload/head/repo/full_name', 'TEXT') as pull_request_event_head_repo_full_name,
  JSON_EXTRACT(src, '/payload/head/label', 'TEXT') as pull_request_event_head_label,
  JSON_EXTRACT(src, '/payload/head/ref', 'TEXT') as pull_request_event_head_ref,
  JSON_EXTRACT(src, '/payload/base/repo/full_name', 'TEXT') as pull_request_event_base_repo_full_name,
  JSON_EXTRACT(src, '/payload/base/label', 'TEXT') as pull_request_event_base_label,
  JSON_EXTRACT(src, '/payload/base/ref', 'TEXT') as pull_request_event_base_ref,
  JSON_EXTRACT(src, '/payload/commits', 'INT') as pull_request_event_commits,
  JSON_EXTRACT(src, '/payload/additions', 'INT') as pull_request_event_additions,
  JSON_EXTRACT(src, '/payload/deletions', 'INT') as pull_request_event_deletions,
  JSON_EXTRACT(src, '/payload/changed_files', 'INT') as pull_request_event_changed_files,
  JSON_EXTRACT(src, '/payload/comments', 'INT') as pull_request_event_comments,
  JSON_EXTRACT(src, '/payload/review_comments', 'INT') as pull_request_event_review_comments,
  JSON_EXTRACT(src, '/payload/author_association', 'TEXT') as pull_request_event_author_association,

  -- issue comment
  JSON_EXTRACT(src, '/payload/issue/number', 'INT') as issue_comment_event_issue_number,
  JSON_EXTRACT(src, '/payload/issue/id', 'INT')::bigint as issue_comment_event_issue_id,
  JSON_EXTRACT(src, '/payload/issue/state', 'TEXT') as issue_comment_event_issue_state,
  JSON_EXTRACT(src, '/payload/issue/locked', 'TEXT')::boolean as issue_comment_event_issue_locked,
  JSON_EXTRACT(src, '/payload/issue/comments', 'INT') as issue_comment_event_issue_comments,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/issue/created_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/issue/created_at', 'TEXT'), 12, 8))::timestamp as issue_comment_event_issue_created_at,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/issue/updated_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/issue/updated_at', 'TEXT'), 12, 8))::timestamp as issue_comment_event_issue_updated_at,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/issue/closed_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/issue/closed_at', 'TEXT'), 12, 8))::timestamp as issue_comment_event_issue_closed_at,
  JSON_EXTRACT(src, '/payload/issue_author_association', 'TEXT') as issue_comment_event_issue_author_association,
  JSON_EXTRACT(src, '/payload/issue/pull_request/html_url', 'TEXT') as issue_comment_event_issue_pull_request_html_url,
  JSON_EXTRACT(src, '/payload/issue/reactions/total_count', 'INT') as issue_comment_event_issue_reactions_total_count,
  JSON_EXTRACT(src, '/payload/issue/reactions/+1', 'INT') as issue_comment_event_issue_reactions_plus_one,
  JSON_EXTRACT(src, '/payload/issue/reactions/-1', 'INT') as issue_comment_event_issue_reactions_minus_one,
  JSON_EXTRACT(src, '/payload/issue/reactions/laugh', 'INT') as issue_comment_event_issue_reactions_laugh,
  JSON_EXTRACT(src, '/payload/issue/reactions/hooray', 'INT') as issue_comment_event_issue_reactions_hooray,
  JSON_EXTRACT(src, '/payload/issue/reactions/confused', 'INT') as issue_comment_event_issue_reactions_confused,
  JSON_EXTRACT(src, '/payload/issue/reactions/heart', 'INT') as issue_comment_event_issue_reactions_heart,
  JSON_EXTRACT(src, '/payload/issue/reactions/rocket', 'INT') as issue_comment_event_issue_reactions_rocket,
  JSON_EXTRACT(src, '/payload/issue/reactions/eyes', 'INT') as issue_comment_event_issue_reactions_eyes,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/comment/created_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/comment/created_at', 'TEXT'), 12, 8))::timestamp as issue_comment_event_comment_created_at,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/comment/updated_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/comment/updated_at', 'TEXT'), 12, 8))::timestamp as issue_comment_event_comment_updated_at,
  JSON_EXTRACT(src, '/payload/comment/reactions/total_count', 'TEXT') as issue_comment_event_comment_reactions_total_count,
  JSON_EXTRACT(src, '/payload/comment/reactions/+1', 'INT') as issue_comment_event_comment_reactions_plus_one,
  JSON_EXTRACT(src, '/payload/comment/reactions/-1', 'INT') as issue_comment_event_comment_reactions_minus_one,
  JSON_EXTRACT(src, '/payload/comment/reactions/laugh', 'INT') as issue_comment_event_comment_reactions_laugh,
  JSON_EXTRACT(src, '/payload/comment/reactions/hooray', 'INT') as issue_comment_event_comment_reactions_hooray,
  JSON_EXTRACT(src, '/payload/comment/reactions/confused', 'INT') as issue_comment_event_comment_reactions_confused,
  JSON_EXTRACT(src, '/payload/comment/reactions/heart', 'INT') as issue_comment_event_comment_reactions_heart,
  JSON_EXTRACT(src, '/payload/comment/reactions/rocket', 'INT') as issue_comment_event_comment_reactions_rocket,
  JSON_EXTRACT(src, '/payload/comment/reactions/eyes', 'INT') as issue_comment_event_comment_reactions_eyes,

  -- delete event
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'DeleteEvent'
      THEN JSON_EXTRACT(src, '/payload/ref', 'TEXT')
    ELSE
      NULL
  END as delete_event_ref,
  CASE
    WHEN JSON_EXTRACT(src, '/type', 'TEXT') == 'DeleteEvent'
      THEN JSON_EXTRACT(src, '/payload/ref_type', 'TEXT')
    ELSE
      NULL
  END as delete_event_ref_type,
  JSON_EXTRACT(src, '/payload/pusher_type', 'TEXT') as delete_event_pusher_type,

  -- pull request review event
  JSON_EXTRACT(src, '/payload/review/id', 'INT')::bigint as pull_request_review_event_id,
  JSON_EXTRACT(src, '/payload/review/commit_id', 'TEXT') as pull_request_review_event_commit_id,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/review/submitted_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/review/submitted_at', 'TEXT'), 12, 8))::timestamp as pull_request_review_event_submitted_at,
  JSON_EXTRACT(src, '/payload/review/state', 'TEXT') as pull_request_review_event_state,
  JSON_EXTRACT(src, '/payload/pull_request/state', 'TEXT') as pull_request_review_event_pull_request_state,
  JSON_EXTRACT(src, '/payload/pull_request/number', 'INT') as pull_request_review_event_pull_request_number,
  TRANSFORM(
        x -> JSON_EXTRACT(x,'/login','String'),
        JSON_EXTRACT_ARRAY_RAW(src, '/payload/pull_request/requested_reviewers')
  ) as pull_request_review_event_pull_request_requested_reviewers,

  -- issues event
  JSON_EXTRACT(src, '/payload/issue/id', 'INT')::bigint as issues_event_id,
  JSON_EXTRACT(src, '/payload/issue/number', 'INT') as issues_event_number,
  JSON_EXTRACT(src, '/payload/issue/state', 'TEXT') as issues_event_state,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/issue/created_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/issue/created_at', 'TEXT'), 12, 8))::timestamp as issues_event_created_at,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/issue/updated_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/issue/updated_at', 'TEXT'), 12, 8))::timestamp as issues_event_updated_at,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/issue/closed_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/issue/closed_at', 'TEXT'), 12, 8))::timestamp as issues_event_closed_at,
  JSON_EXTRACT(src, '/payload/issue/reactions/total_count', 'INT') as issues_event_reactions_total_count,
  JSON_EXTRACT(src, '/payload/issue/reactions/+1', 'INT') as issues_event_reactions_plus_one,
  JSON_EXTRACT(src, '/payload/issue/reactions/-1', 'INT') as issues_event_reactions_minus_one,
  JSON_EXTRACT(src, '/payload/issue/reactions/laugh', 'INT') as issues_event_reactions_laugh,
  JSON_EXTRACT(src, '/payload/issue/reactions/hooray', 'INT') as issues_event_reactions_hooray,
  JSON_EXTRACT(src, '/payload/issue/reactions/confused', 'INT') as issues_event_reactions_confused,
  JSON_EXTRACT(src, '/payload/issue/reactions/heart', 'INT') as issues_event_reactions_heart,
  JSON_EXTRACT(src, '/payload/issue/reactions/rocket', 'INT') as issues_event_reactions_rocket,
  JSON_EXTRACT(src, '/payload/issue/reactions/eyes', 'INT') as issues_event_reactions_eyes,

  -- fork event
  JSON_EXTRACT(src, '/payload/forkee/full_name', 'TEXT') as fork_event_full_name,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/forkee/created_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/forkee/created_at', 'TEXT'), 12, 8))::timestamp as fork_event_created_at,

  -- pull request review comment
  JSON_EXTRACT(src, '/payload/comment/diff_hunk', 'TEXT') as pull_request_review_comment_event_diff_hunk,
  JSON_EXTRACT(src, '/payload/comment/path', 'TEXT') as pull_request_review_comment_event_path,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/comment/created_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/comment/created_at', 'TEXT'), 12, 8))::timestamp as pull_request_review_comment_event_created_at,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/comment/updated_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/comment/updated_at', 'TEXT'), 12, 8))::timestamp as pull_request_review_comment_event_updated_at,
  JSON_EXTRACT(src, '/payload/comment/id', 'INT')::bigint as pull_request_review_comment_event_id,
  JSON_EXTRACT(src, '/payload/comment/in_reply_to', 'INT')::bigint pull_request_review_comment_event_in_reply_to,
  JSON_EXTRACT(src, '/payload/pull_request/number', 'INT') as pull_request_review_comment_event_number,

  -- commit comment
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/comment/created_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/comment/created_at', 'TEXT'), 12, 8))::timestamp as commit_comment_event_created_at,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/comment/updated_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/comment/updated_at', 'TEXT'), 12, 8))::timestamp as commit_comment_event_updated_at,
  JSON_EXTRACT(src, '/payload/comment/reactions/total_count', 'INT') as commit_comment_event_reactions_total_count,
  JSON_EXTRACT(src, '/payload/comment/reactions/+1', 'INT') as commit_comment_event_reactions_plus_one,
  JSON_EXTRACT(src, '/payload/comment/reactions/-1', 'INT') as commit_comment_event_reactions_minus_one,
  JSON_EXTRACT(src, '/payload/comment/reactions/laugh', 'INT') as commit_comment_event_reactions_laugh,
  JSON_EXTRACT(src, '/payload/comment/reactions/hooray', 'INT') as commit_comment_event_reactions_hooray,
  JSON_EXTRACT(src, '/payload/comment/reactions/confused', 'INT') as commit_comment_event_reactions_confused,
  JSON_EXTRACT(src, '/payload/comment/reactions/heart', 'INT') as commit_comment_event_reactions_heart,
  JSON_EXTRACT(src, '/payload/comment/reactions/rocket', 'INT') as commit_comment_event_reactions_rocket,
  JSON_EXTRACT(src, '/payload/comment/reactions/eyes', 'INT') as commit_comment_event_reactions_eyes,

 
  -- release event
  JSON_EXTRACT(src, '/payload/release/tag_name', 'TEXT') as release_event_tag_name,
  JSON_EXTRACT(src, '/payload/release/tag_commitish', 'TEXT') as release_event_target_commitish,
  JSON_EXTRACT(src, '/payload/release/pre_release', 'TEXT')::boolean as release_event_pre_release,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/release/created_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/release/created_at', 'TEXT'), 12, 8))::timestamp as release_event_created_at,
  CONCAT(SUBSTR(JSON_EXTRACT(src, '/payload/release/published_at', 'TEXT'), 1, 10), ' ', SUBSTR(JSON_EXTRACT(src, '/payload/release/published_at', 'TEXT'), 12, 8))::timestamp as release_event_published_at,

  -- member event
  JSON_EXTRACT(src, '/payload/member/login', 'TEXT') as member_event_member_login,
  JSON_EXTRACT(src, '/payload/member/type', 'TEXT') as member_event_member_type,

  -- gollum event
  TRANSFORM(
        x -> JSON_EXTRACT(x,'/page_name','String'),
        JSON_EXTRACT_ARRAY_RAW(src, '/payload/pages')
  ) as gollum_event_page_names,
  TRANSFORM(
        x -> JSON_EXTRACT(x,'/title','String'),
        JSON_EXTRACT_ARRAY_RAW(src, '/payload/pages')
  ) as gollum_event_titles,
  TRANSFORM(
        x -> JSON_EXTRACT(x,'/html_url','String'),
        JSON_EXTRACT_ARRAY_RAW(src, '/payload/pages')
  ) as gollum_event_html_urls,
  TRANSFORM(
        x -> JSON_EXTRACT(x,'/action','String'),
        JSON_EXTRACT_ARRAY_RAW(src, '/payload/pages')
  ) as gollum_event_actions,

  -- required for continuous updates
  source_file_name,
  source_file_timestamp
FROM ex_gharchive        
WHERE JSON_EXTRACT(src, '/actor/id', 'INT') IS NOT NULL AND
      JSON_EXTRACT(src, '/repo/id', 'int') IS NOT NULL AND
      %s;
