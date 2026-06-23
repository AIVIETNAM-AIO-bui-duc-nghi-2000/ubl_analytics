# Data Dictionary: Glamira UBL Data

**Project:** User Behavioral Log (UBL) Analytics
**Source System:** Countly Log / MongoDB
**Last Updated:** 2026-06-22

| Field Name | Data Type | Description | System / ETL Notes |
| :--- | :--- | :--- | :--- |
| **`_id`** | ObjectId | Auto-generated primary key by MongoDB. | Excluded from analytical pipelines and downstream aggregations. |
| **`time_stamp`** | Integer | The exact time the event was logged. | Stored as **Unix Epoch Time** (seconds since 1970). Requires casting to standard UTC Timestamp during the ETL/Transformation phase. |
| **`ip`** | String | Originating IPv4 address of the user event. | Target feature for the IP-to-Geolocation mapping process (enrichment). |
| **`collection`** | String | Represents the User Event Type. | Misnomer in source data (not a DB collection). Values include `view_product_detail`, `add_to_cart_action`. Crucial for event-based filtering. |
| **`product_id`** | String | Unique identifier for the catalog item. | May be missing/null for non-product-specific events. |
| **`current_url`** | String | The specific URL of the product page viewed. | Target source for web scraping product metadata (enrichment). |