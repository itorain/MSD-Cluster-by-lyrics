from sys import argv

class SongInfo(object):
  def __init__(self, artist, title, tempo, year, key, loudness, timeSig):
    self.artist = artist
    self.title = title
    self.tempo = tempo
    self.year = year
    self.key = key
    self.loudness = loudness
    self.timeSig = timeSig

  def __str__(self):
    return "{"+self.artist+","+self.title+","+str(self.tempo)+","+str(self.year)+","+str(self.key)+","+str(self.loudness)+","+str(self.timeSig)+"}"

class Track(object):
  def __init__(self, trackID, songInfo):
    self.trackID = trackID
    self.songInfo = songInfo
    self.wordList = {}

  def __str__(self):
    return str(self.songInfo)+"\t"+str(self.wordList)

  def addWord (self, word, count):
    if word in self.wordList:
      self.wordList[word] = self.wordList[word] + count
    else:
      self.wordList[word] = count

def readInfoFile (fileName):
  tracks = {}
  f = open(fileName)
  for l in f:
    info1 = l.strip().split(',')
    info = [x for x in info1 if x != ""]
    if len(info) == 8:
      sI = SongInfo(info[1],info[2],info[3],info[4],info[5],info[6],info[7])
      t = Track(info[0],sI)
      tracks[t.trackID] = t
  f.close()
  return tracks

def readWordIndexFile(fileName):
  words = {}
  index = 1
  f = open(fileName)
  for l in f:
    l = l.strip().split(',')
    for w in l:
      words[index] = w
      index = index + 1
  return words

def readWordCountFile(tracks, words, fileName):
  f = open(fileName)
  for l in f:
    l = l.strip().split(',')
    if l[0] in tracks:
      for w in l[2:]:
        temp = w.split(':')
        if len(temp) == 2 and int(temp[0]) in words and int(temp[1]) > 0:
          tracks[l[0]].addWord(words[int(temp[0])], int(temp[1]))
  return tracks

def cleanUpTracks(tracks):
  newTracks = {}
  for x in tracks:
    if len(tracks[x].wordList) != 0:
      newTracks[x] = tracks[x]
  return newTracks

def mergeDics(x,y):
  z = x.copy()
  z.update(y)
  return z

def connectAllWords(tracks, words):
  generalWordList = set(words.values())
  for x in tracks:
    keyWordList = set((tracks[x].wordList).keys())
    diffList = generalWordList - keyWordList
    diff = {}
    for each in diffList:
      diff[each] = 0
    tracks[x].wordList = mergeDics(diff,tracks[x].wordList)
  return tracks



if __name__ == '__main__':
  tracks = readInfoFile(argv[1])
  #print tracks
  words = readWordIndexFile(argv[2])
  tracks = readWordCountFile(tracks, words, argv[3])
  tracks = cleanUpTracks(tracks)
  #tracks = connectAllWords(tracks, words)
  for x in tracks:
    print tracks[x]
