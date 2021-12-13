from flask import Flask

def create_app(config = None):
    app = Flask(__name__)
    if config is not None:
        app.config.update(config)

    from flask_app.route.main import main_bp

    app.register_blueprint(main_bp)
    
    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug = True)