MATCH (content:Post)
RETURN 'Post' AS type, COUNT(content) AS totalCount

UNION ALL

MATCH (content:Comment)
RETURN 'Comment' AS type, COUNT(content) AS totalCount