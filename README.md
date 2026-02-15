1. Project Background
Overview
The Bangumi Data Pipeline is an automated data collection and visualization system designed to extract, process, and visualize user collection data from Bangumi (bgm.tv), a popular Chinese anime, manga, and game database platform. This project addresses the need for users to analyze their media consumption patterns and preferences across different categories.
Problem Statement
Bangumi users currently lack:
•	Centralized data storage: User collection data is scattered across the platform
•	Analytical capabilities: Limited built-in tools for analyzing viewing habits and preferences
•	Visualization options: No easy way to visualize collection statistics and trends
•	Data portability: Difficulty in exporting and using collection data in other applications
Solution
This pipeline provides:
1.	Automated data collection from Bangumi API
2.	Structured storage in MySQL database
3.	Advanced analytics capabilities
4.	Visualization through Notion integration
5.	Incremental updates for efficient data synchronization
2. Project Deliverables
Technical Deliverables
2.1 Data Collection Module (bangumi_data_ingestion.py)
•	✅ API Integration: Connects to Bangumi API v0
•	✅ Category Management: Handles multiple subject types (books, anime, music) and collection statuses
•	✅ Error Handling: Implements retry logic with exponential backoff
•	✅ Data Processing: Parses complex data structures (infobox, tags)
•	✅ Export Capabilities: Generates Excel and CSV files with multiple sheets
2.2 Database Integration Module (data_loading_to_mysql_database.py)
•	✅ MySQL Integration: Connects to MySQL database with UTF-8 support
•	✅ Data Transformation: Converts Python data types to MySQL-compatible formats
•	✅ Incremental Loading: Implements efficient data synchronization
•	✅ Logging System: Comprehensive error logging and monitoring
•	✅ Connection Management: Context managers for reliable database operations
2.3 Notion Integration Module (notion_data_ingestion.py)
•	✅ Notion API Integration: Connects to Notion API
•	✅ Database Creation: Programmatically creates Notion databases
•	✅ Data Synchronization: Syncs MySQL data to Notion
•	✅ Property Mapping: Converts data types to Notion-compatible formats
•	✅ Error Recovery: Retry logic for API failures
2.4 Output Files
•	Excel Files:
o	bangumi_collections_by_category.xlsx (3 sheets: raw, analytics, summary)
•	CSV Files:
o	bangumi_raw_by_category.csv
o	bangumi_analytics_by_category.csv
o	bangumi_category_summary.csv
•	Log Files:
o	data-loading-errors.log
o	notion_output.json
Functional Deliverables
2.5 Core Features
•	Multi-category Collection: Collects data across books, anime, and music
•	Status Tracking: Tracks "want to watch", "watched", "watching", and "on hold" statuses
•	Metadata Extraction: Extracts tags, ratings, and additional metadata
•	Progress Monitoring: Real-time progress tracking during data collection
•	Data Quality Checks: Validates data completeness and consistency
2.6 Database Schema
bangumi_analytics database:
├── fact_view_logs (analytics data)
├── fact_view_logs_raw (raw API data)
└── fact_view_logs_incremental (incremental updates)
2.7 Notion Database Structure
Bangumi Database:
├── subject_id (Title)
├── subject_type (Number)
├── collection_type (Number)
├── name_cn (Rich Text)
├── score (Number)
├── rank (Number)
├── collection_total (Number)
├── created_at (Rich Text)
├── updated_at (Date)
├── eps (Number)
├── air_date (Rich Text)
└── all_tags (Rich Text)
3. Assumptions, Risks, and Next Steps
3.1 Assumptions
Technical Assumptions
1.	API Stability: Bangumi API will remain stable and accessible
2.	Rate Limits: API rate limits (429 responses) are handled appropriately
3.	Data Consistency: API response structure remains consistent
4.	Authentication: User has valid Bangumi account and API access token
5.	Network Connectivity: Stable internet connection during data collection
6.	Database Access: MySQL database is accessible and properly configured
7.	Notion API Access: Valid Notion integration token with appropriate permissions
Data Assumptions
1.	Data Completeness: All user collections are accessible via API
2.	Data Accuracy: API provides accurate and up-to-date information
3.	Character Encoding: UTF-8 encoding handles all Chinese/Japanese characters
4.	Time Zones: All timestamps are converted to timezone-naive format
3.2 Risks and Mitigations
High Priority Risks
Risk	Impact	Probability	Mitigation
API Changes	High	Medium	Regular monitoring, version pinning
Rate Limiting	Medium	High	Exponential backoff, request throttling
Data Loss	High	Low	Regular backups, transaction management
Network Issues	Medium	Medium	Retry logic, connection pooling
Authentication Failure	High	Low	Token validation, error handling
Medium Priority Risks
Risk	Impact	Probability	Mitigation
Data Volume	Medium	Medium	Pagination, batch processing
Character Encoding	Medium	Low	UTF-8 validation, encoding checks
Memory Usage	Low	Medium	Streaming processing, chunking
Notion API Limits	Medium	Medium	Rate limiting, batch operations
3.3 Next Steps
Short-term (Next 1-2 Weeks)
1.	Testing & Validation
o	Unit tests for all modules
o	Integration testing with real data
o	Performance testing with large datasets
o	Error scenario testing
2.	Documentation
o	User guide for configuration
o	Troubleshooting guide
o	API documentation
o	Deployment instructions
3.	Monitoring
o	Implement health checks
o	Add performance metrics
o	Set up alerting system
o	Log analysis tools
Medium-term (Next 1-2 Months)
1.	Enhanced Features
o	Dashboard for data visualization
o	Scheduled data collection (cron jobs)
o	Data export to additional formats (JSON, XML)
o	Advanced analytics (trend analysis, recommendations)
2.	Scalability Improvements
o	Docker containerization
o	Cloud deployment options
o	Database optimization
o	Caching implementation
3.	Integration Extensions
o	Additional visualization tools (Tableau, Power BI)
o	Mobile app integration
o	Social media sharing
o	Email reporting
Long-term (Next 3-6 Months)
1.	Advanced Analytics
o	Machine learning for recommendations
o	Sentiment analysis on reviews
o	Predictive modeling for viewing habits
o	Comparative analysis with community data
2.	Platform Expansion
o	Support for additional media platforms
o	Cross-platform data aggregation
o	API for third-party integrations
o	Mobile application development
3.	Community Features
o	Social sharing capabilities
o	Community comparisons
o	Group analytics
o	Public dashboards
4. Data Source Fields and Requirements
4.1 Bangumi API Data Structure
Core API Endpoint
GET /v0/users/{username}/collections
Parameters:
- subject_type: 1=Books, 2=Anime, 3=Music, 4=Games, 6=Real
- type: 1=Want, 2=Watched, 3=Watching, 4=On Hold, 5=Dropped
- limit: 100 (max per request)
- offset: Pagination offset
4.2 Raw Data Fields (Complete API Response)
Field	Type	Description	Source
user_id	String	Bangumi username	Configuration
subject_id	Integer	Unique subject identifier	API response
subject_type	Integer	1-6 (see mapping)	API parameter
collection_type	Integer	1-5 (see mapping)	API parameter
created_at	DateTime	When item was added to collection	API response
updated_at	DateTime	Last update time	API response
ep_status	Integer	Episode progress	API response
vol_status	Integer	Volume progress	API response
name	String	Original name	Subject data
name_cn	String	Chinese name	Subject data
score	Float	User rating (1-10)	Subject data
rank	Integer	Popularity rank	Subject data
collection_total	Integer	Total collections	Subject data
eps	Integer	Total episodes	Subject data
volumes	Integer	Total volumes	Subject data
date	String	Air date	Subject data
type	Integer	Subject type	Subject data
short_summary	String	Brief description	Subject data
tags	List	User tags	Subject data
tags_raw	JSON	Raw tags data	Subject data
infobox_raw	JSON	Raw infobox data	Subject data
4.3 Analytics Data Fields (Processed)
Field	Type	Description	Transformation
subject_id	Integer	Unique identifier	Direct mapping
subject_type	Integer	Media type	Direct mapping
collection_type	Integer	Collection status	Direct mapping
name_cn	String	Display name	name_cn or name
score	Float	Rating	Direct mapping
rank	Integer	Popularity	Direct mapping
collection_total	Integer	Total collections	Direct mapping
created_at	DateTime	Collection date	Timezone conversion
updated_at	DateTime	Update date	Timezone conversion
eps	Integer	Episode count	Direct mapping
air_date	String	Release date	Direct mapping
director	String	Director name	Infobox extraction
studio	String	Production studio	Infobox extraction
country	String	Country of origin	Infobox extraction
publisher	String	Publisher	Infobox extraction
author	String	Author	Infobox extraction
tag_1_name	String	Top tag 1	Tag extraction
tag_1_count	Integer	Tag 1 count	Tag extraction
tag_2_name	String	Top tag 2	Tag extraction
tag_2_count	Integer	Tag 2 count	Tag extraction
tag_3_name	String	Top tag 3	Tag extraction
tag_3_count	Integer	Tag 3 count	Tag extraction
tag_4_name	String	Top tag 4	Tag extraction
tag_4_count	Integer	Tag 4 count	Tag extraction
tag_5_name	String	Top tag 5	Tag extraction
tag_5_count	Integer	Tag 5 count	Tag extraction
all_tags	String	All tags concatenated	Tag aggregation
4.4 Data Quality Requirements
Requirement	Validation	Action on Failure
Completeness	Required fields not null	Log warning, skip record
Consistency	Data types match schema	Type conversion, log error
Accuracy	Values within valid ranges	Range validation, default values
Timeliness	Data freshness	Timestamp validation
Uniqueness	No duplicate subject_id	Deduplication, log warning
4.5 Performance Requirements
Metric	Target	Measurement
API Response Time	< 2 seconds	Request timing
Data Processing	< 5 seconds per 1000 records	Processing timing
Database Insert	< 10 seconds per 1000 records	Insert timing
Memory Usage	< 500 MB	Memory monitoring
Error Rate	< 1%	Error logging
5. Business Requirements
5.1 Current Business Background
Industry Context
•	Media Consumption Tracking: Growing demand for personal media analytics
•	Data-driven Decisions: Users want insights into their viewing habits
•	Cross-platform Integration: Need for unified media tracking across platforms
•	Personalization: Customized recommendations based on viewing history
Market Needs
1.	Personal Analytics: Users want to understand their media consumption patterns
2.	Collection Management: Need for organized tracking of watched/planned media
3.	Social Sharing: Ability to share media preferences and achievements
4.	Discovery: Finding new content based on existing preferences
5.2 Business Objectives
Primary Objectives
1.	Data Centralization
o	Aggregate scattered media collection data
o	Create single source of truth for user media consumption
o	Enable cross-platform data analysis
2.	User Insights
o	Provide analytics on viewing habits and preferences
o	Identify patterns and trends in media consumption
o	Enable data-driven media selection decisions
3.	Visualization & Accessibility
o	Make data accessible through user-friendly interfaces
o	Enable easy sharing and collaboration
o	Support multiple access methods (web, mobile, API)
Secondary Objectives
1.	Community Building
o	Enable comparison with community trends
o	Facilitate social interactions around media
o	Build user engagement through data sharing
2.	Monetization Opportunities
o	Premium analytics features
o	API access for developers
o	Data partnerships with media companies
3.	Platform Expansion
o	Support for additional media types
o	Integration with other platforms
o	Internationalization support
5.3 Target Audiences
Primary Users
User Type	Needs	Use Cases
Casual Viewers	Simple tracking, basic stats	Track watched shows, get recommendations
Media Enthusiasts	Detailed analytics, trends	Analyze viewing patterns, compare with friends
Content Creators	Audience insights, trends	Understand viewer preferences, content planning
Researchers	Data analysis, patterns	Study media consumption trends, cultural analysis
Secondary Users
User Type	Needs	Use Cases
Developers	API access, integration	Build apps, integrate with other services
Media Companies	Audience insights, trends	Content planning, marketing strategies
Academic Researchers	Data for studies, analysis	Cultural studies, media psychology research
User Personas
1.	Alex (Casual User)
o	Demographics: 25, office worker
o	Goals: Track watched anime, get recommendations
o	Pain Points: Forgets what they've watched, hard to find new shows
o	Usage: Weekly check-ins, basic stats viewing
2.	Maya (Power User)
o	Demographics: 30, data analyst
o	Goals: Detailed analytics, trend analysis
o	Pain Points: No advanced analytics tools, manual tracking
o	Usage: Daily updates, custom reports, data export
3.	David (Content Creator)
o	Demographics: 28, YouTuber
o	Goals: Audience insights, content planning
o	Pain Points: No data on viewer preferences
o	Usage: Weekly analytics, trend monitoring
5.4 Success Metrics
Quantitative Metrics
Metric	Target	Measurement Period
User Adoption	1000 active users	Monthly
Data Accuracy	99.5% accuracy	Weekly
System Uptime	99.9% availability	Monthly
Processing Speed	< 5 minutes for full sync	Per operation
Error Rate	< 0.5%	Weekly
Qualitative Metrics
1.	User Satisfaction
o	Survey scores (NPS > 50)
o	Feature usage patterns
o	Support ticket volume
2.	Data Utility
o	Report generation frequency
o	Data export usage
o	Integration adoption
3.	Business Impact
o	Time saved for users
o	Improved media discovery
o	Enhanced user engagement
6. Business Glossary
6.1 Core Terminology
Media Types
Term	Definition	Example
Subject	Any media item in Bangumi database	Anime series, book, music album
Subject Type	Category of media item	1=Books, 2=Anime, 3=Music, 4=Games, 6=Real
Collection	User's relationship with a subject	Want to watch, Watched, Watching, On Hold, Dropped
Collection Status
Term	Code	Definition
Want to Watch	1	Planned to watch/read/listen
Watched	2	Completed watching/reading
Watching	3	Currently watching/reading
On Hold	4	Temporarily paused
Dropped	5	Stopped watching/reading
Data Categories
Term	Definition	Usage
Raw Data	Unprocessed API response	Data validation, debugging
Analytics Data	Processed, structured data	Reporting, visualization
Metadata	Additional information about subjects	Enhanced analysis, filtering
Infobox	Structured data fields from Bangumi	Director, studio, country info
6.2 Technical Terms
System Components
Term	Definition	Purpose
Data Pipeline	End-to-end data processing system	Automates data flow from source to destination
API Client	Software that interacts with Bangumi API	Fetches user collection data
ETL Process	Extract, Transform, Load	Data processing workflow

