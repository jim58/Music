#!/usr/bin/python
# 2019  Jim Harris <ja_harris@rogers.com>
"""Music.py - music file tag management"""

import os
import re
import mutagen
import wave
import datetime
import copy

configdir = '/home/jim/conf/'
song_formats = ['mp3', 'flac', 'ogg', 'wma', 'm4a']
list_tags = ['trkn', 'disk']
bool_tags = ['cpil', 'pgap']

Verbose = False

DB_fields = [
        'db_album',             # string
        'db_albumartist',       # string
        'db_artist',            # string
        'db_comment',           # string
        'db_composer',          # string
        'db_date',              # yyyy-mm-dd | yyyy
        'db_disknumber',        # int
        'db_disktotal',         # int
        'db_genre',             # string
        'db_encoder',           # string
        'db_length',            # int
        'db_publisher',         # string
        'db_title',             # string
        'db_track',             # int
        'db_tracknumber',       # int
        'db_tracktotal',        # int
        'db_info_bitrate',      # int
        'db_info_channels',     # int
        'db_info_length',       # float
        'db_info_sample_rate',  # int
        'db_stat_size',         # int
        'db_stat_time',         # yyyy-mm-dd hh:mm:ss
        'db_pathname',          # absolute pathname of the file
        'db_format',            # flac/mp3/wma/m4a/ogg
        ]

DB_field_defs = {
        'db_album':            'character varying(200)',
        'db_albumartist':      'character varying(200)',
        'db_artist':           'character varying(200)',
        'db_comment':          'character varying(200)',
        'db_composer':         'character varying(200)',
        'db_date':             'character varying(20)',
        'db_disknumber':       'integer',
        'db_disktotal':        'integer',
        'db_genre':            'character varying(200)',
        'db_encoder':          'character varying(200)',
        'db_length':           'integer',
        'db_publisher':        'character varying(200)',
        'db_title':            'character varying(200)',
        'db_track':            'integer',
        'db_tracknumber':      'integer',
        'db_tracktotal':       'integer',
        'db_info_bitrate':     'integer',
        'db_info_channels':    'integer',
        'db_info_length':      'integer',  # really, it's a float, but its been converted
        'db_info_sample_rate': 'integer',
        'db_stat_size':        'integer NOT NULL',
        'db_stat_time':        'timestamptz NOT NULL',
        'db_pathname':         'character varying(400) NOT NULL',
        'db_format':           'character varying(20) NOT NULL',
        }


def pverbose(comment):
    if Verbose:
        print(comment)

class song_record:
    '''
        song = song_record()
        song.configure(tags_configure_object)
        song.show_includes()
        song.show_excludes()
        song.load(songfilename)
        song.display()
        some_db_method(song)
    '''

    def __init__(self):
        '''instantiate a song record as key/value pairs for database insertion
        
        '''

        self.tags = {}
        for key in DB_fields:
            self.tags[key] = ''
        self.extras = {}
        self.config = False  # configure() sets this to a tags_config() object

    def display(self):
        '''display contents of the song record'''
        print("Tags:")
        for key in self.tags.keys():
            print(" Tags {:20} : {}".format(key, self.tags[key]))
        if self.extras.keys():
            print()
        for key in self.extras.keys():
            print(" Extras {:18} : {}".format(key, self.extras[key]))

    def configure(self,tags_config):
        '''configure the song record'''
        pverbose("tags_config type: {}".format(type(tags_config)))
        if tags_config.includes:
            pverbose(" .. includes for: {}".format(tags_config.includes.keys()))
            pass
        else:
            pverbose(" .. no includes")
            assert False
            return(False)
        if tags_config.excludes:
            pverbose(" .. excludes for: {}".format(tags_config.excludes.keys()))
            pass
        else:
            pverbose(" .. no excludes")
            assert False
            return(False)
        self.config = tags_configure()
        self.config = copy.deepcopy(tags_config)

    def show_includes(self, fmt='all'):
        '''display included tags and their mapping values (self.config.includes)'''
        if fmt in song_formats:  # only show fmt tags/values
            print()
            print("Includes {}:".format(fmt))
            for key in self.config.includes[fmt].keys():
                print("  {:30} :: {}".format(key, self.config.includes[fmt][key]))
            return()
            
        for fmt in song_formats:
            print()
            print("Includes {}:".format(fmt))
            for key in self.config.includes[fmt].keys():
                print("  {:30} :: {}".format(key, self.config.includes[fmt][key]))

    def show_excludes(self, fmt='all'):
        '''display excluded tags'''
        if fmt in song_formats:  # only show fmt tags/values
            print()
            print("Excludes {}:".format(fmt))
            for key in self.config.excludes[fmt].keys():
                print("  {:30} :: {}".format(key, self.config.excludes[fmt][key]))
            return()
            

        for fmt in song_formats:
            print()
            print("Excludes {}:".format(fmt))
            for key in self.config.excludes[fmt].keys():
                print("  {}".format(key))




    def load(self, filename):
        '''load songfile values into the song record
        
        song_record.load(filename)
        '''
        assert os.path.exists(filename)  # ugly fail if the file doesn't exist

        song_stat = os.stat(filename)
        pathname = os.path.abspath(filename)
        self.tags['db_pathname'] = pathname
        self.tags['db_stat_size'] = song_stat.st_size
        self.tags['db_stat_time'] = datetime.datetime.fromtimestamp(int(round(song_stat.st_mtime)))

        try:
            songtags = mutagen.File(filename)
        except:
            return(0)

        pverbose("song_record.load(songtags): type(songtags): {}".format(type(songtags)))
        fmt = 'none'
        if type(mutagen.asf.ASF()) == type(songtags):
            pverbose(" type is wma")
            fmt = 'wma'
        elif type(mutagen.flac.FLAC()) == type(songtags):
            pverbose(" type is flac")
            fmt = 'flac'
        elif type(mutagen.mp3.MP3()) == type(songtags):
            pverbose(" type is mp3")
            fmt = 'mp3'
        elif type(mutagen.mp4.MP4()) == type(songtags):
            pverbose(" type is m4a")
            fmt = 'm4a'
        elif type(mutagen.oggvorbis.OggVorbis()) == type(songtags):
            pverbose(" type is ogg")
            fmt = 'ogg'
        else:
            fmt = 'none'
            print("unknown file format")

        assert fmt in song_formats  # ugly fail if it's not a song file

        self.tags['db_format'] = fmt
        self.tags['db_info_bitrate'] = songtags.info.bitrate  # int
        self.tags['db_info_channels'] = songtags.info.channels  # int
        self.tags['db_info_length'] = "{:1.0f}".format(songtags.info.length)  # float
        self.tags['db_info_sample_rate'] = songtags.info.sample_rate  # int

        for key in songtags.keys():
            pverbose(" .. load(): {}".format(key))
            if key in self.config.excludes[fmt].keys():
                pass
            elif key in self.config.includes[fmt].keys():
                mapkey = self.config.includes[fmt][key]
                if key == 'TRCK':
                    try:
                        (a, b) = str(songtags[key]).split('/')
                        self.tags['db_tracknumber'] = a
                        self.tags['db_tracktotal'] = b
                    except:
                        self.tags['db_track'] = songtags[key]
                elif key == 'TLEN':
                    x = int(songtags[key][0])
                    x = int(x/1000)
                elif key == "WM/EncodingTime":
                    self.tags[mapkey] = str(songtags[key])[0:10]
                elif type(songtags[key]) == type([]):
                    if 'trkn' == key:
                        (a, b) = songtags[key][0]
                        self.tags['db_tracknumber'] = a
                        self.tags['db_tracktotal'] = b
                    elif 'disk' == key:
                        (a, b) = songtags[key][0]
                        self.tags['db_disknumber'] = a
                        self.tags['db_disktotal'] = b
                    elif key in list_tags:
                        (a, b) = songtags[key][0]
                        self.tags[mapkey] = "{}/{}".format(a, b)
                        self.tags[mapkey] = str(songtags[key][0])
                    elif mapkey in self.tags.keys():
                        self.tags[mapkey] = str(songtags[key][0])
                    else:
                        self.extras[mapkey] = str(songtags[key][0])
                elif mapkey in self.tags.keys():
                    self.tags[mapkey] = str(songtags[key][0])
                else:
                    self.extras[mapkey]= str(songtags[key])


class tags_configure:
    def __init__(self):
        '''load tag info from configuration files

           self.includes = { 'mp3': {}, 'flac': {}, 'ogg': {}, 'wma': {}, 'm4a': {} }
           self.excludes = { 'mp3': {}, 'flac': {}, 'ogg': {}, 'wma': {}, 'm4a': {} }


           config = tags_configure()
           song.configure(config)

        '''

        self.includes = {}
        self.excludes = {}

        for fmt in song_formats:
            include_file = configdir + 'include.' + fmt
            exclude_file = configdir + 'exclude.' + fmt
            self.includes[fmt] = {}
            self.excludes[fmt] = {}

            if os.path.exists(include_file):
                config = self.load_config(include_file)
                for key in config.keys():
                    self.includes[fmt][key] = config[key]
        
            if os.path.exists(exclude_file):
                config = self.load_config(exclude_file)
                for key in config.keys():
                    self.excludes[fmt][key] = config[key]
                    if key in self.includes[fmt].keys():
                        self.includes[fmt].pop(key)


    def load_config(self, filename):
        """parse a config file
        
           returns a dictionary
        """

        tagnames = {}

        pverbose('CONFIG FILE: "{}"'.format(filename))
        try:
            f = open(filename)
        except:
            return(tagnames)
        lines = f.readlines()
        f.close()

        for line in lines:
            line = line.replace('\n', '')
            try:
                (a, b) = line.split()
                pverbose("CONFIG:     {:28} > {}".format(a,b))
            except:
                a = line
                b = line
                pverbose("CONFIG:     {:28} = {}".format(a,b))
            tagnames[a] = b
        return(tagnames)

    def show_includes(self):
        '''display included tags and their mapping values'''

        for fmt in song_formats:
            print()
            print("Includes {}:".format(fmt))
            for key in self.includes[fmt].keys():
                print("  {:30} :: {}".format(key, self.includes[fmt][key]))

    def show_excludes(self):
        '''display excluded tags'''

        for fmt in song_formats:
            print()
            print("Excludes {}:".format(fmt))
            for key in self.excludes[fmt].keys():
                print("  {}".format(key))



if __name__ == '__main__':

    import Database

    samples = {
        "wma": "/var/www/MusicDB/Kathryn Williams/Old Low Light/04 - Devices.wma",
        "mp3": "/var/www/MusicDB/Mighty Popo/African Guitar Summit/11 Mwembo.mp3",
        "wma": "/var/www/MusicDB/Kathryn Williams/Old Low Light/04 - Devices.wma",
        "m4a": "/var/www/MusicDB/Eleanor McEvoy/Snapshots - m4a/08 She Had It All.m4a",
        "ogg": "/var/www/MusicDB/Dido/Safe Trip Home/01 - Don't Believe in Love.ogg",
        "flac": "/var/www/MusicDB/James Taylor/Hourglass/01 James Taylor - Line 'Em Up.flac"
    }


    #Verbose = True
    config = tags_configure()
    #config.verbose = True
    #config.configure()
    #config.show_includes()
    #config.show_excludes()

    songrec = song_record()
    songrec.configure(config)
    #print()
    #print(songrec.tags)
    #songrec.display()
    print()

    sqltabledef = Database.table_config()
    sqltabledef.fields = DB_fields
    sqltabledef.field_defs = DB_field_defs

    db = Database.db_connect()
    db.drop()
    db.create(sqltabledef)

    #print("songrec config:")
    #songrec.show_includes('mp3')
    #songrec.show_excludes('wma')
    #print()

    #samples = {}
    for fmt in samples.keys():
        filename = samples[fmt]
        #print()
        #print(filename)
        #print()
        songrec = song_record()
        songrec.configure(config)
        songrec.load(filename)
        #songrec.display()
        db.insert(songrec, sqltabledef)
        
    fields = { 'db_album': "Safe Trip Home", 'db_artist': 'Dido' }

    db.lookup(fields, sqltabledef)
