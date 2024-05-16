def connect(http, credentials):
    """Creates SAASHR auth token from credential dictionary.

    Accepts dictionary and makes a post request to saashr API login endpoint, returns
    token string.

    See detailed API documentation and usage examples:
    `https://secure.saashr.com/ta/docs/rest/public/`

    Typical usage example:
        inova_auth = connect(credentials)

    Args:
        'credentials' (dict): Dictionary carry credentials. 
    
    Returns:
        Inova authentication bearer string.

    Dictionary Construction:
        {
            "credentials": {
                "username": "employee",
                "password": "pass",
                "company": "short_name"
                },
            "Api-Key": "SAASHR api key",
            "cookie": "cookie (idk if this is required.)"
        }
    """

    url = "https://secure.saashr.com/ta/rest/v1/login"
    payload = {'credentials': credentials['credentials']}

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Api-Key": credentials["Api-Key"],
        "cookie": credentials["cookie"]
    }

    return http.post(url, json=payload, headers=headers).json()['token']

def get_inova_data_v2(http, bearer, cid, endpoint, **qp):
    # create api url
    cid = f"!{cid}"
    url = f"https://secure.saashr.com/ta/rest/v2/companies/{cid}/{endpoint}"
    # Creates the query string from single key:value pair dictionaries query parameters.
    # Note: Theres nothing saying you cant just add them all to qp1 but IMO it reduces readability.
    querystring = {}
    for i,j in qp.items():
        if i!="":
            querystring.update({i:j})
    payload = ""
    headers = {
        "Accept": "application/json",
        "Authentication": f"Bearer {bearer}",
    }
    # Requests data from inova API url
    response = http.get(url, data=payload, headers=headers, params=querystring)
    return response


def get_inova_data_report(http, bearer, cid, cookie, endpoint):

    url = f"https://secure.saashr.com/ta/rest/v1/report/global/{endpoint}"

    querystring = {"company": cid}
    if endpoint == 'REPORT_TIME_ENTRY_SUMMARY':
        payload = {
            "fields": ["EmplEmployeeId",
                "EmplFirstName",
                "EmplLastName",
                "TimeEntryDate",
                "TotalHours",
                "NumEntries",
                "TimesheetStart",
                "TimeEntryCostCenter1FullPath",
                "TimeEntryCostCenter2FullPath",
                "TimeEntryCostCenter3FullPath",
                "TimeEntryCostCenter4FullPath",
                "TimeEntryIsPTO",
                "TimeEntryTimeOffName"],
            "selectors": [
                {
                "name": "PPDate",
                "parameters": {
                    "RangeType": "1",
                    "CalendarType": "40",
                            "CalendarNDays": "30",
                            "CalendarNDaysIncludeToday": "1"
            }
            }]
        }
    else:
        payload = {
        } 

    headers = {
        "cookie": cookie,
        "Accept": "text/csv",
        "Content-Type": "application/json",
        "Authentication": f"Bearer {bearer}"
    }

    response = http.post(url, json=payload, headers=headers, params=querystring)
    return response