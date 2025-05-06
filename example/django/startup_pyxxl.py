import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

if __name__ == "__main__":
    import django

    django.setup()
    from mysite.pyxxl import pyxxl_app

    pyxxl_app.run_executor()
