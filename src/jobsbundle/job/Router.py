class Router:
    def __init__(
        self,
        databricks_url: str,
        routes: dict,
    ):
        self.__databricks_url = databricks_url
        self.__routes = routes

    def generate_url(self, route_name: str, **kwargs):
        if route_name not in self.__routes:
            raise Exception(f"Route not defined: {route_name}")

        databricks_url = self.__databricks_url if self.__databricks_url[-1] != "/" else self.__databricks_url[:-1]

        return databricks_url + self.__routes[route_name].format(**kwargs)
