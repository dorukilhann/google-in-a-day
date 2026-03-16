# Production Roadmap

For production scale, this project should move from a single-process crawler and indexer to a distributed architecture with separate crawl workers, a queue, persistent storage, and a scalable search index. Before deployment, it should add robots.txt support, politeness limits by domain, retry handling, logging, monitoring, and stronger failure recovery.

The search layer should be separated from the crawler and backed by durable indexed storage such as Elasticsearch, OpenSearch, or a custom inverted index service. For higher scale, add sharding, caching, observability, ranking improvements, and containerized deployment with orchestration.
