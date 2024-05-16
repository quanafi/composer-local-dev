import python_libraries.inova.inova_library as inova_library
import python_libraries.inova.helper_functions as helpers

import json
from time import sleep
class Inova:
    """Class for accessing Inova Api functions."""

    def __init__ (self,
                  inova_credentials_json

    ):
        """   
        """

        self._http = helpers.http_session()

        self._inova_credentials_json = inova_credentials_json
        self.set_inova_credentials() # Sets self._inova_credentials
        self._inova_bearer_token = inova_library.connect(self._http, self._inova_credentials)

    def get_inova_credentials(self):
        """Getter for Inova credential file."""
        return self._inova_credentials_json
    
    def set_inova_credentials(self):
        """Setter for Inova Credential Dictionary."""
        try:
            self._inova_credentials = self.get_inova_credentials()
        except Exception as error:
            print(f"(URGENT) Unable to set Inova credentials from path.\n{error}")

    def reconnect(self):
        self._inova_bearer_token = inova_library.connect(self._http, self._inova_credentials)

    def endpoint(self, endpoint, method='get', version=2, **querystring):
        if method=='get' and version==2:
            data = inova_library.get_inova_data_v2(
                                        self._http, 
                                        self._inova_bearer_token, 
                                        self._inova_credentials["credentials"]["company"], 
                                        endpoint, 
                                        **querystring)
            if data.status_code == 200:
                return data.json()
            elif data.status_code == 403:
                print("Reconnecting....")
                self.reconnect()
                return self.endpoint(endpoint, method, version, **querystring)
            elif data.status_code == 503:
                print(f"Data Throttled, waiting 60 seconds.")
                sleep(60)
                print("Reconnecting....")
                return self.endpoint(endpoint, method, version, **querystring)
            else:
                print(f"Status Code: {data.status_code} ERROR")

        elif method=='report' and version==1:
            data = inova_library.get_inova_data_report(
                                            self._http,
                                            self._inova_bearer_token, 
                                            self._inova_credentials["credentials"]["company"],
                                            self._inova_credentials["cookie"],
                                            endpoint)
            if data.status_code == 200:
                return data.text
            elif data.status_code == 403:
                print("Reconnecting....")
                self.reconnect()
                return self.endpoint(endpoint, method, version, **querystring)
            elif data.status_code == 503:
                print(f"Data Throttled, waiting 60 seconds.")
                sleep(60)
                print("Reconnecting....")
                self.reconnect()
                return self.endpoint(endpoint, method, version, **querystring)
            else:
                print(f"Status Code: {data.status_code} ERROR")
        else:
            error_message = f"""(URGENT) Endpoint doesn't have the following combination:
                method: {method} and version: {version}"""
            print(error_message)
            return error_message

