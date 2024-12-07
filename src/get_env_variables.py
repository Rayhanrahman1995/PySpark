import os


os.environ['env'] = 'QA'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
env = os.environ['env']

appName = 'youtube pyspark'

current = os.getcwd()

