#!/usr/bin/env groovy

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.zip.ZipEntry
import java.util.zip.ZipFile
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream
import groovy.xml.XmlUtil

def options = new HashMap();

try {
  if (project && project.properties) {
    if (project.properties.containsKey("assembly")) {
      options.p = project.properties.assembly
    }
    if (project.properties.containsKey("system")) {
      options.h = project.properties.system
    }
    if (project.properties.containsKey("bigDataPlugin")) {
      options.b = project.properties.bigDataPlugin
    }
  }
} catch (MissingPropertyException e) {
  try {
    def cli = new CliBuilder(usage: 'updateAssembly.groovy')
    cli.p('pdi archive', args: 1)
    cli.h('hsp system repo', args: 1)
    cli.b('big data plugin', args: 1)
    options = cli.parse(args)
    if (!options || !options.p || !options.h) {
      cli.usage()
    }
  } catch (MissingPropertyException e2) {
    println "Unable to get args from project properties or cli"
  }
}

if (!options || !options.p || !options.h) {
  System.exit(1)
}

def origArchive = options.p + '.orig'
def newArchive = options.p

if (!new File(newArchive).renameTo(origArchive)) {
  throw new Exception("Unable to rename archive")
}

def findFeatureFiles(zipFile, fileNamePattern) {
  return Collections.list(zipFile.entries()).collect {
    return it.name
  }.grep {
    return it.find(fileNamePattern)
  }
}

def getFeatureBundles(zipFile, featureFile) {
  def bundles = new HashSet()
  getAndParseFeatureFile(zipFile, featureFile).feature.bundle.each {
    bundles.add(it.text())
  }
  return bundles
}

def getAndParseFeatureFile(zipFile, featureFile) {
  def entry = zipFile.getEntry(featureFile)
  def inputStream = zipFile.getInputStream(entry)
  try {
    return new XmlParser().parse(inputStream)
  } finally {
    inputStream.close()
  }
}

def locateBundle(zipFile, systemRepoPath, gav) {
  assert gav.startsWith('mvn:')
  def splitGav = gav.substring(4).split('/')
  def result = systemRepoPath + splitGav[0].replace('.', '/') + '/' + splitGav[1] + '/' + splitGav[2] + '/' + splitGav[1] + '-' + splitGav[2]
  if (splitGav.length > 4) {
    result = result + '-' + splitGav[4]
  }
  if (splitGav.length > 3) {
    result = result + '.' + splitGav[3]
  } else {
    result = result + '.jar'
  }
  if (zipFile.getEntry(result) != null) {
    return result
  }
  return null
}

def removeVersion(gav) {
  assert gav.startsWith('mvn:')
  def secondSlash = gav.indexOf('/', gav.indexOf('/') + 1)
  def thirdSlash = gav.indexOf('/', secondSlash + 1)
  def result = gav.substring(0, secondSlash)
  if (thirdSlash >= 0) {
    result += gav.substring(thirdSlash)
  }
  return result
}

def copyFromHspMap = new ConcurrentHashMap()
def filesToRemove = Collections.newSetFromMap(new ConcurrentHashMap())
def featureFileXmlMap = new ConcurrentHashMap()

def hspSystemRepoZipFile = new java.util.zip.ZipFile(new File(options.h))
try {
  def pdiArchiveZipFile = new java.util.zip.ZipFile(new File(origArchive))
  def pdiArchiveZipFileSize = pdiArchiveZipFile.size()
  try {
    def featureFiles = findFeatureFiles(pdiArchiveZipFile, /.*pentaho-big-data-plugin-osgi-.*-features.xml/)
    def hspFeatureFile = findFeatureFiles(hspSystemRepoZipFile, /.*pentaho-big-data-plugin-osgi-.*-features.xml/)
    assert hspFeatureFile.size() == 1
    hspFeatureFile = hspFeatureFile[0]

    def missingBundles = new HashSet()
    def filesFromHspZip = new HashMap()
    getFeatureBundles(hspSystemRepoZipFile, hspFeatureFile).each {
      def systemRepoPath = 'pentaho-hsp-assembly/'
      def bundleLocation = locateBundle(hspSystemRepoZipFile, systemRepoPath, it)
      if (bundleLocation == null) {
        missingBundles.add(it)
      } else {
        filesFromHspZip.put(bundleLocation, bundleLocation.substring(systemRepoPath.length()))
      }
    }
    // Find out what feature files we'll be overwriting
    featureFiles.each {
      def systemRepoPath = it.replaceAll('system/karaf/system.*', 'system/karaf/system/')
      println "Replacing ${it} with ${hspFeatureFile}"
      def bundlesToKeep = new HashMap()
      def hspFeatureXml = getAndParseFeatureFile(hspSystemRepoZipFile, hspFeatureFile)
      getFeatureBundles(pdiArchiveZipFile, it).each {
        def bundleLocation = locateBundle(pdiArchiveZipFile, systemRepoPath, it)

        def featureBundle = it
        def featureBundleWithoutVersion = removeVersion(featureBundle)
        // Look for required bundles not in new system repo, update new feature file
        def missingMatch = missingBundles.find {
          return removeVersion(it).equals(featureBundleWithoutVersion)
        }
        if (missingMatch) {
          hspFeatureXml.feature.bundle.each {
            if (it.text().equals(missingMatch)) {
              it.value = featureBundle
            }
          }
        } else {
          filesToRemove.add(bundleLocation)
        }
      }
      featureFileXmlMap.put(it, hspFeatureXml)
      filesFromHspZip.each { k, v ->
        copyFromHspMap.put(systemRepoPath + v, k)
      }
      filesToRemove.add(it)
    }
  } finally {
    pdiArchiveZipFile.close()
  }

  def zipEntryNamesAdded = new HashSet()
  def queue = new LinkedBlockingQueue(10)
  def bigDataPluginDirs = Collections.newSetFromMap(new ConcurrentHashMap())
  // Here we read the assembly zip in in a thread so we can write at the same time
  Thread.start {
    def pdiArchiveZipInputStream = new ZipInputStream(new BufferedInputStream(new FileInputStream(origArchive)))
    try {
      def zipEntry = pdiArchiveZipInputStream.nextEntry
      while(zipEntry != null) {
        // Omit big data plugin if one was specified on the command line
        if (options.b && zipEntry.name.contains('plugins/pentaho-big-data-plugin/')) {
          bigDataPluginDirs.add(zipEntry.name.substring(0, zipEntry.name.indexOf('plugins/pentaho-big-data-plugin/')) + 'plugins/')
        } else if (zipEntry.isDirectory()) {
          queue.put([zipEntry.name, true])
          // Omit anything to be copied from new system repo, anything that was replaced by a different version in the system repo
        } else if (!copyFromHspMap.containsKey(zipEntry.name) && !filesToRemove.contains(zipEntry.name)) {
          def baos = new ByteArrayOutputStream()
          baos << pdiArchiveZipInputStream
          queue.put([zipEntry.name, false, baos.toByteArray()])
        }
        zipEntry = pdiArchiveZipInputStream.nextEntry
      }
    } finally {
      pdiArchiveZipInputStream.close()
    }
    queue.put([])
  }
  def pdiArchiveZipOutputStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(newArchive)))
  try {
    println "Processing ${pdiArchiveZipFileSize} entries from ${newArchive}"
    def processed = 0
    def progress = 0
    def lastProgress = 0
    def zipEntryTuple = queue.take()
    // Write entries that weren't filtered from pdi assembly
    while(zipEntryTuple.size() > 0) {
      pdiArchiveZipOutputStream.putNextEntry(new ZipEntry(zipEntryTuple[0]))
      if (!zipEntryTuple[1]) {
        pdiArchiveZipOutputStream << new ByteArrayInputStream(zipEntryTuple[2])
      }
      zipEntryNamesAdded.add(zipEntryTuple[0])
      processed++
      progress = (processed * 100).intdiv(pdiArchiveZipFileSize)
      if (progress != lastProgress) {
        lastProgress = progress
        print "\r${progress}% done processing ${newArchive}"
      }
      zipEntryTuple = queue.take()
    }

    // Write new system repo files
    def keys = new ArrayList(copyFromHspMap.keySet())
    Collections.sort(keys)
    keys.each {
      def sourceLocation = copyFromHspMap.get(it)
      def splitDestLocation = it.split('/')
      for(int i = 0; i < splitDestLocation.length - 1; i++) {
        def potentialZipEntry = splitDestLocation[0..i].join('/') + '/'
        if (zipEntryNamesAdded.add(potentialZipEntry)) {
          pdiArchiveZipOutputStream.putNextEntry(new ZipEntry(potentialZipEntry))
        }
      }
      pdiArchiveZipOutputStream.putNextEntry(new ZipEntry(it))
      def hspInputStream = hspSystemRepoZipFile.getInputStream(hspSystemRepoZipFile.getEntry(sourceLocation))
      try {
        pdiArchiveZipOutputStream << hspInputStream
      } finally {
        hspInputStream.close()
      }
    }

    // Overwrite replaced feature files
    featureFileXmlMap.each { k, v ->
      pdiArchiveZipOutputStream.putNextEntry(new ZipEntry(k))
      XmlUtil.serialize(v, pdiArchiveZipOutputStream)
    }

    // Write big data plugin if it was provided
    bigDataPluginDirs.each {
      def bigDataInputStream = new ZipInputStream(new BufferedInputStream(new FileInputStream(options.b)))
      try {
        def bigDataEntry = bigDataInputStream.nextEntry
        while(bigDataEntry != null) {
          pdiArchiveZipOutputStream.putNextEntry(new ZipEntry(it + bigDataEntry.name))
          if (!bigDataEntry.isDirectory()) {
            pdiArchiveZipOutputStream << bigDataInputStream
          }
          bigDataEntry = bigDataInputStream.nextEntry
        }
      } finally {
        bigDataInputStream.close()
      }
    }
  } finally {
    try { pdiArchiveZipOutputStream.close() } catch (Exception e) {}
  }
} finally {
  hspSystemRepoZipFile.close()
}
println ""
