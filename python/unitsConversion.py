from lxml import etree

"""
Class: uomconversionFunctions
Purpose: Uses a conversion XML file to look up a from units of measurement and to units of measurement conversion 
routine. If one is found, will evaluate the function and return the result. The XML file needs to be formated with 
valid python code.
"""
class uomconversionFunctions:
  """
  Function: __init__
  Purpose: Initializes the class
  Parameters: 
    xmlConversionFile is the full path to the XML file to use for the conversions.
  """
  def __init__(self, xmlConversionFile=None):
    self.xmlConversionFile = xmlConversionFile
    
  def setXMLConversionFile(self, xmlConversionFile):
    self.xmlConversionFile = xmlConversionFile
    
  """
  Function: measurementConvert
  Purpose: Attempts to find a conversion formula using the passed in fromUOM and toUOM variables.
  Parameters:
    value is the floating point number to try and convert.
    fromUOM is the units of measurement the value is currently in.
    toUOM is the units of measurement we want to value to be converted to.
  Return:
    If a conversion routine is found, then the converted value is returned, otherwise None is returned.
  """
  def measurementConvert(self, value, fromUOM, toUOM):
    xmlTree = etree.parse(self.xmlConversionFile)
    
    convertedVal = ''
    xmlTag = "//unit_conversion_list/unit_conversion[@id=\"%s_to_%s\"]/conversion_formula" % (fromUOM, toUOM)
    unitConversion = xmlTree.xpath(xmlTag)
    if( len(unitConversion) ):     
      conversionString = unitConversion[0].text
      conversionString = conversionString.replace( "var1", ("%f" % value) )
      convertedVal = float(eval( conversionString ))
      return(convertedVal)
    return(None)
  """
  Function: getConversionUnits
  Purpose: Given a unit of measurement in a differing measurement system and the desired measurement system, returns uom in the desired measurement system.
  Parameters:
    uom is the current unit of measurement we want to convert.
    uomSystem is the desired measurement system we want to conver the uom into.
  Return:
    if the uomSystem is found and the uom is in the uomSystem, returns the conversion uom
  """
  def getConversionUnits(self, uom, uomSystem):
    if( uomSystem == 'en' ):
      if( uom == 'm'):
        return('ft')
      elif( uom == 'm_s-1'):
        return('mph')
      elif( uom == 'celsius' ):
        return('fahrenheit')
      elif(uom == 'cm_s-1'):
        return('mph')
      elif(uom == 'mph'):
        return('knots')
    return('')
      