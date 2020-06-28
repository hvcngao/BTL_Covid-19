from databases.db_connection import db
from marshmallow_sqlalchemy import ModelSchema

class GeoDistributionWorld(db.Model):
    __tablename__ ="geo_distribution_world"
    id = db.Column(db.Integer, primary_key=True)
    cases = db.Column(db.Float, nullable=False)
    deaths = db.Column(db.Float, nullable=False)
    countries = db.Column(db.String(1000), nullable=False)
    geold = db.Column(db.String(1000), nullable=False)
    country_code = db.Column(db.String(1000), nullable=False)
    pop_data_2018 = db.Column(db.Float, nullable=False)
    active_date = db.Column(db.Date, nullable=True)
    day = db.Column(db.String(400), nullable=True)
    month = db.Column(db.String(400), nullable=True)
    year = db.Column(db.Integer, nullable=True)


class GeoDistributionWorldSchema(ModelSchema):
    class Meta:
        model = GeoDistributionWorld
        sqla_session = db.session
