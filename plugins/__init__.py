from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

from plugins import blueprints
from plugins import operators


# Defining the plugin class
class EuropythonPlugin(AirflowPlugin):
    name = "europython_plugin"
    operators = [
        operators.BookData,
        operators.Predict,
        operators.Decide
    ]
    flask_blueprints = [blueprints.TriggerBlueprint]
