
from os import path
import optparse
import re

models  = []
metrics = []
params  = []
def processRow(row, curBlockInfo, modelCount, curModelNdx):
  cleanRow = row.strip()
  if(curBlockInfo['type'] == "model"):
    modelParts = cleanRow.split(':')
    models.append(modelParts[1].strip())
    
  elif(curBlockInfo['type'] == "metrics"):
    metricParts = cleanRow.split(':')
    metricFound = False
    metricName = metricParts[0].strip()
    if(len(metricName)):
      for metric in metrics:
        if(metricName in metric):
          metricFound = True
          break
      if(metricFound == False):
        metric = {metricName : []}
        metrics.append(metric)
      metric[metricName].append(metricParts[1].strip())
    
  elif(curBlockInfo['type'] == "params"):
    #Params have 3 lines, the Parameter Name:, the headers, the values.
    if(curBlockInfo['currentState'] == 1):
      if(cleanRow.find('Parameter Name') != -1):
        parts = cleanRow.split(':')
        paramFound = False
        for param in params:
          if(parts[1].strip() in param):
            paramFound = True
            break
        if(paramFound == False):
          params.append({parts[1].strip() : []})
        curBlockInfo['curParam'] = parts[1].strip()
        curBlockInfo['currentState'] = 2
    elif(curBlockInfo['currentState'] == 2):
      if(cleanRow.find('Coefficient') != -1):
         curBlockInfo['headers'] = cleanRow.split('\t')
         curBlockInfo['currentState'] = 3
    else:
      if(curBlockInfo['currentState'] == 3):
        curBlockDataParts = cleanRow.split('\t')
        curParam = None
        for param in params:
          if curBlockInfo['curParam'] in param:
            curParam = param[curBlockInfo['curParam']]
            break
        for x in range(0,len(curBlockDataParts)):
          headerName = curBlockInfo['headers'][x].strip()
          varFound = False          
          for var in curParam:
            if(headerName in var):
              varFound = True
              break
          if(varFound == False):
            var = {headerName : ['Not Used'] * modelCount}
            curParam.append(var)
          #var[headerName].append(curBlockDataParts[x].strip())
          var[headerName][curModelNdx] = curBlockDataParts[x].strip()
          
        curBlockInfo['currentState'] = 1
        curBlockInfo['curParam'] = None
        del curBlockInfo['headers'][:]
def main():
  parser = optparse.OptionParser()
  parser.add_option("-i", "--InputFile", dest="inputFile",
                    help="Full path to file to process." )
  (options, args) = parser.parse_args()
  if( options.inputFile == None ):
    parser.print_usage()
    parser.print_help()
    exit(-1)
  

  vbFile = open(options.inputFile, 'rU')
  outFilename,ext = path.splitext(options.inputFile)
  outFilename = outFilename + "_metrics.csv"
  outFile = open(outFilename, 'w')
  blocks = [
    {
      "type" : "model",
      "processStartText" : True,
      "inBlock" : False,
      "startText" : re.compile("Model: "),
      "endText" : re.compile("\n"),
    },
    {
      "type" :"metrics",
      "processStartText" : False,
      "inBlock" : False, 
      "startText" : re.compile("All Evaluation Metrics:"),
      "endText" : re.compile("\n")
    },
    {
      "type" : "params", 
      "currentState" : 1,
      "processStartText" : True,
      "inBlock" : False, 
      "startText" : re.compile("Parameter Name:"),
      "endText" : re.compile("\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*")
    }            
  ]
  modelCount = 0
  curModelNdx = 0
  
  #Run a pass through to determine how many models we have.
  for row in vbFile:
    curBlockInfo = blocks[0]
    if(curBlockInfo['inBlock'] == False):
      if(curBlockInfo['startText'].match(row) != None):
        curBlockInfo['inBlock'] = True
        modelCount += 1
    else:
      if(curBlockInfo['endText'].match(row) != None):
        curBlockInfo['inBlock'] = False
  
  #Go back to start of file.
  vbFile.seek(0,0)
  curBlock = 0
  for row in vbFile:
    curBlockInfo = blocks[curBlock]
    if(curBlockInfo['inBlock'] == False):
      if(curBlockInfo['startText'].match(row) != None):
        curBlockInfo['inBlock'] = True
        if(curBlockInfo['processStartText'] == False):
          continue
        else:
          processRow(row, curBlockInfo, modelCount, curModelNdx)
    else:
      if(curBlockInfo['endText'].match(row) != None):
        if(curBlockInfo['type'] == 'params'):
          curModelNdx += 1
        curBlockInfo['inBlock'] = False
        curBlock += 1
        if(curBlock >= len(blocks)):
          curBlock = 0
        continue
      else:
        processRow(row, curBlockInfo, modelCount, curModelNdx)
  vbFile.close()

  #outFile.write(",," + ",".join(models))
  outFile.write(",,")
  for model in models:
    outFile.write("\"" + model + "\",") 
  outFile.write("\n")
  outFile.write("\n")

  for metric in metrics:
    keys = metric.keys()    
    outFile.write(keys[0] + ',')
    outFile.write("," + ",".join(metric[keys[0]]))
    outFile.write("\n")
  outFile.write("\n")
  
  for param in params:
    keys = param.keys()
    outFile.write("\"" + keys[0] + "\"")
    results = param[keys[0]]
    for var in results:
      keys = var.keys()
      if(keys[0] == "Coefficient" or keys[0] == "P-Value"):
        outFile.write(',' + keys[0] + ',')
        outFile.write(",".join(var[keys[0]]))
        outFile.write('\n')
  outFile.write('\n')

  outFile.close()
if __name__ == '__main__':
  main()