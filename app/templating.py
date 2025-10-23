from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="app/templates")
templates.env.globals.update(
    {
        "hasattr": hasattr,
        "getattr": getattr,
        "len": len,
    }
)
