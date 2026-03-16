# Product Requirement Document

## Project
Google in a Day

## Objective
Build a functional web crawler and real-time search engine from scratch.

## Core Requirements
- Recursive crawling from an origin URL to maximum depth k
- Visited set so no page is crawled twice
- Back pressure using a bounded queue or rate control
- Use native language functionality where practical
- Search can run while indexing is active
- Thread-safe concurrent data structures
- Search returns (relevant_url, origin_url, depth)
- Simple relevance ranking such as keyword frequency or title match

## Non-Goals
- Full internet-scale crawling
- Advanced production ranking
- Distributed infrastructure in version 1

## Deliverables
- Working codebase
- README.md
- product_prd.md
- recommendation.md
