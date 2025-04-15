from airflow.plugins_manager import AirflowPlugin

# Import your custom classes (ensure paths match your structure)
from http_polling_plugin.operators.http_polling_operator import HttpPollingDeferrableOperator
from http_polling_plugin.triggers.http_polling_trigger import HttpPollingTrigger

# Define the plugin class
class HttpPollingPlugin(AirflowPlugin):
    # Define a name for the plugin
    name = "http_polling_plugin"

    # List the operators provided by this plugin
    operators = [HttpPollingDeferrableOperator]

    # List the triggers provided by this plugin
    triggers = [HttpPollingTrigger]

    # You can also add hooks, sensors, macros, etc. here if needed
    # hooks = []
    # sensors = []
    # macros = []
    # admin_views = []
    # flask_blueprints = []
    # menu_links = []
    # appbuilder_views = []
    # appbuilder_menu_items = []
    # global_operator_extra_links = []
    # operator_extra_links = []
