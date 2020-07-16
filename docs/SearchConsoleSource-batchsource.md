# Search Console Batch Source

Description
-----------

Reads given dimensions from a Google Search Console api in JSON format. Outputs records with dimensions provided in config.

Use Case
--------

This source is used whenever you need to read from a Google Search Console.

Properties
----------

##### Credentials:

Plugin supports two types of authentication methods:

**OAuth Client** - requires the following properties:

| Property Name | Description |
| ------------- |:-------------|
|**Client ID** | Google Search Console client ID for authentication |
|**Client Secret:** | Google Search Console client secret for authentication |
|**Client Access Token:** | Google Search Console client access token |
|**Client Application Name:** | Google Search Console application name |

**Service Account** - requires only service file path

| Property Name | Description |
| ------------- |:-------------|
|**Service Account File Path:** | Path to json file containing service account key |

#### Source Configuration
| Property Name | Description |
| ------------- |:-------------|
|**Sites:** | List of sites to query from. Can be set to All Site's - will retrieve list of sites from Search Console API|
|**Site List:** | [Optional] List of sites to query from. Is active only when Site URL List option is selected| 
|**Start Date:** | Start date in ​YYYY-MM-DD format, in PT time (UTC - 7:00/8:00)|
|**End Date:** | Start date in ​YYYY-MM-DD format, in PT time (UTC - 7:00/8:00)|
|**Dimension(s):** | List of comma separated dimensions to read from Google Search Console|
|**Number of Splits:** | Desired number of splits to divide the number of sites into when reading from Search Console. Fewer splits may be created if the number sites cannot be divided into the desired number of splits.|


