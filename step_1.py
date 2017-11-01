
def normalize_country_code(element):
  COUNTRY_CODE_MAP = {
    "CANADA" : "CDN",
    "CDN" : "CDN",
    "UNITED STATES" : "USA",
    "USA" : "USA",
    "UNITED STATES OF AMERICA" : "USA",
    "MEXICO": "MX",
    "MX": "MX"
  }
  element[1] = COUNTRY_CODE_MAP[element[1].upper()]
  return element

if __name__ == "__main__":
  result = normalize_country_code(["2017-10-02", "Canada", "1717", "tv"])
  print "result %s" % result
