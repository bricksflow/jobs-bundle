import re


def fill_template(template: str, replacements: dict):
    def convert(m):
        placeholder = m.group(0)[1:-1]

        if placeholder not in replacements:
            raise Exception("Value for placeholder {" + placeholder + "} not defined")

        return str(replacements[placeholder])

    placeholders = set(re.findall(r"{(\w+)}", template))
    double_placeholders = set(re.findall(r"{{(\w+)}}", template))

    placeholders -= double_placeholders

    if not placeholders:
        return template

    pattern = re.compile("|".join(map(lambda x: "{" + x + "}", placeholders)))

    return pattern.sub(convert, template)
