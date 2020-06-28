from databases.db_connection import db
from marshmallow_sqlalchemy import ModelSchema

class GlobalPerCountry(db.Model):
    __tablename__ ="v_covid_global_per_country"

    country = db.Column(db.String(100) ,primary_key=True)
    confirmed_case = db.Column(db.Integer ,nullable=True)
    deaths = db.Column(db.Integer, nullable=True)
    confirmed_cored = db.Column(db.Integer, nullable=True)



class GlobalPerCountrySchema(ModelSchema):
    class Meta:
        model = GlobalPerCountry
        sqla_session = db.session
    def make(self, data, **kwargs):
        return GlobalPerCountry(**data)

