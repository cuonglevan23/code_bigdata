import pandas as pd
from datetime import datetime

class Crime:
    """
    Model lớp Crime dùng để chuẩn hóa dữ liệu trước khi chèn vào MongoDB.
    """
    def __init__(self, x, y, ID, CaseNumber, Date, Block, IUCR, PrimaryType, Description,
                 LocationDescription, Arrest, Domestic, Beat, District, Ward, CommunityArea,
                 FBICode, XCoordinate, YCoordinate, Year, UpdatedOn, ZIPCode):
        self.x = x
        self.y = y
        self.ID = ID
        self.CaseNumber = CaseNumber
        self.Date = self._parse_date(Date)
        self.Block = Block
        self.IUCR = IUCR
        self.PrimaryType = PrimaryType
        self.Description = Description
        self.LocationDescription = LocationDescription
        self.Arrest = self._parse_bool(Arrest)
        self.Domestic = self._parse_bool(Domestic)
        self.Beat = Beat
        self.District = District
        self.Ward = Ward
        self.CommunityArea = CommunityArea
        self.FBICode = FBICode
        self.XCoordinate = XCoordinate
        self.YCoordinate = YCoordinate
        self.Year = Year
        self.UpdatedOn = self._parse_date(UpdatedOn)
        self.ZIPCode = ZIPCode

    @staticmethod
    def _parse_date(date_str):
        if pd.isna(date_str):
            return None
        try:
            return datetime.strptime(str(date_str).strip(), "%m/%d/%Y %I:%M:%S %p")
        except Exception:
            return None

    @staticmethod
    def _parse_bool(value):
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() == "true"

    def to_dict(self):
        result = {
            "x": self.x,
            "y": self.y,
            "ID": self.ID,
            "CaseNumber": self.CaseNumber,
            "Date": self.Date.isoformat() if self.Date else None,
            "Block": self.Block,
            "IUCR": self.IUCR,
            "PrimaryType": self.PrimaryType,
            "Description": self.Description,
            "LocationDescription": self.LocationDescription,
            "Arrest": self.Arrest,
            "Domestic": self.Domestic,
            "Beat": self.Beat,
            "District": self.District,
            "Ward": self.Ward,
            "CommunityArea": self.CommunityArea,
            "FBICode": self.FBICode,
            "XCoordinate": self.XCoordinate,
            "YCoordinate": self.YCoordinate,
            "Year": self.Year,
            "UpdatedOn": self.UpdatedOn.isoformat() if self.UpdatedOn else None,
            "ZIPCode": self.ZIPCode
        }
        return result

    @classmethod
    def from_series(cls, row):
        # Bỏ qua nếu không có ZIPCode
        if pd.isna(row.get("ZIPCode")):
            return None
        return cls(
            x=row.get("x"),
            y=row.get("y"),
            ID=row.get("ID"),
            CaseNumber=row.get("CaseNumber"),
            Date=row.get("Date"),
            Block=row.get("Block"),
            IUCR=row.get("IUCR"),
            PrimaryType=row.get("PrimaryType"),
            Description=row.get("Description"),
            LocationDescription=row.get("LocationDescription"),
            Arrest=row.get("Arrest"),
            Domestic=row.get("Domestic"),
            Beat=row.get("Beat"),
            District=row.get("District"),
            Ward=row.get("Ward"),
            CommunityArea=row.get("CommunityArea"),
            FBICode=row.get("FBICode"),
            XCoordinate=row.get("XCoordinate"),
            YCoordinate=row.get("YCoordinate"),
            Year=row.get("Year"),
            UpdatedOn=row.get("UpdatedOn"),
            ZIPCode=row.get("ZIPCode")
        )
