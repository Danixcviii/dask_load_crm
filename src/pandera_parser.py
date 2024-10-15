

import pandera as pa
from datetime import datetime
import re

class PanderaParser(object):
    
    def validate_datetime(self, date: str, fmt:str) -> bool:
        try:
            date, *_ = date.split()
            datetime.strptime(date, fmt)
            return True
        except: return False

    def validate_email(self, email: str) -> bool:
        return bool(re.match(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(email)))


    def validate_only_digits(self, value: str | int) -> bool:
        return bool(re.match(r'^\d+$', str(value)))
    
    def parse_schema(self, colrules: dict) -> pa.DataFrameSchema:
        return pa.DataFrameSchema(
            {colname: self.parse(rule) for colname, rule in colrules.items()}
        )

    def parse(self, rule:str) -> pa.Column:
        required, *other_rules = rule.split("|")
        if other_rules[0].startswith("digits_between"):
            numbers = other_rules[0].split(":")[1]
            min_length, max_length = map(int, numbers.split(","))
            checks = [pa.Check.str_length(min_length, max_length),
                      pa.Check(lambda s: s.apply(self.validate_only_digits), error='It should contain only numbers')]
            
        elif other_rules[0] == "date":
            checks = [pa.Check(lambda s: s.apply(self.validate_datetime, args=(r'%Y-%m-%d', )),
                               error='Invalid date format YYY-mm-dd')]
        elif other_rules[0] == "string":
            min_length = int(other_rules[1].split(":")[1])
            max_length = int(other_rules[2].split(":")[1])
            checks = [pa.Check.str_length(min_length, max_length)]

        elif other_rules[0] == "time":
            checks = [pa.Check(lambda s: s.apply(self.validate_datetime, args=(r'%H:%M:%S', )),
                               error='Invalid time format HH:MM:SS')]
            
        elif other_rules[0] == "email":
            min_length = int(other_rules[1].split(":")[1])
            max_length = int(other_rules[2].split(":")[1])
            checks = [pa.Check.str_length(min_length, max_length),
                      pa.Check(lambda s: s.apply(self.validate_email), error='It is not a valid email format')]

        return pa.Column(pa.String, checks, required=required, nullable=(not required))    