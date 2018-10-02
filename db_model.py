from sqlalchemy import Column, Float, String, text
from sqlalchemy.dialects.mysql import INTEGER, TIMESTAMP, TINYINT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SensorDatum(Base):
        __tablename__ = 'sensor_data'

        id = Column(INTEGER(11), primary_key=True, unique=True)
        timestamp_created = Column(TIMESTAMP(fsp=3), nullable=False, unique=True, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"))
        ACK = Column(TINYINT(4), nullable=False)
        ADR = Column(TINYINT(4), nullable=False)
        AppEUI = Column(String(45), nullable=False)
        CHAN = Column(INTEGER(11), nullable=False)
        CLS = Column(INTEGER(11), nullable=False)
        RAD_CODR = Column(String(45), nullable=False)
        DeviceID = Column(String(45), nullable=False)
        RAD_FREQ = Column(Float(asdecimal=True), nullable=False)
        RAD_LSNR = Column(Float(asdecimal=True), nullable=False)
        MHDR = Column(String(45), nullable=False)
        MODU = Column(String(45), nullable=False)
        OPTS = Column(String(45))
        PORT = Column(INTEGER(11), nullable=False)
        RFCH = Column(Float(asdecimal=True), nullable=False)
        RAD_RSSI = Column(Float(asdecimal=True), nullable=False)
        RAD_SEQN = Column(INTEGER(11), nullable=False)
        Size = Column(INTEGER(11), nullable=False)
        timestamp_node = Column(Float(asdecimal=True), nullable=False)
        Payload = Column(String(255), nullable=False)
        MsgID = Column(String(45), nullable=False)
        V1 = Column(Float(asdecimal=True))
        A1 = Column(Float(asdecimal=True))
        V2 = Column(Float(asdecimal=True))
        A2 = Column(Float(asdecimal=True))
        V3 = Column(Float(asdecimal=True))
        A3 = Column(Float(asdecimal=True))
        kWIII = Column(Float(asdecimal=True))
        kvarLIII = Column(Float(asdecimal=True))
        PFLIII = Column(Float(asdecimal=True))
        WeatherCon = Column(String(45))
        Temperature = Column(Float(asdecimal=True))
        Pressure = Column(Float(asdecimal=True))
        Humidity = Column(Float(asdecimal=True))
        City = Column(String(45))
        WindSpeed = Column(Float(asdecimal=True))
        TimestampWeather = Column(Float(asdecimal=True))

