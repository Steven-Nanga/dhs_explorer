"""Register all route blueprints on the app."""

from .dashboard import bp as dashboard_bp
from .upload import bp as upload_bp
from .explore import bp as explore_bp
from .search import bp as search_bp
from .analysis import bp as analysis_bp
from .compare import bp as compare_bp
from .data_api import bp as data_api_bp
from .manage import bp as manage_bp


def register_all(app):
    for blueprint in (
        dashboard_bp,
        upload_bp,
        explore_bp,
        search_bp,
        analysis_bp,
        compare_bp,
        data_api_bp,
        manage_bp,
    ):
        app.register_blueprint(blueprint)
