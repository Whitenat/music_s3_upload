require 'aws-sdk'
require 'aws-sdk-dynamodb'
#assume the role
#arn:aws:iam::829928713858:role/s3-bucket-role
NO_SUCH_BUCKET = "The bucket '%s' does not exist!"

role_credentials = Aws::AssumeRoleCredentials.new(
  client: Aws::STS::Client.new,
  role_arn: "arn:aws:iam::829928713858:role/s3-bucket-role",
  role_session_name: "s3-upload-session"
)

s3 = Aws::S3::Client.new(region: "us-east-2", credentials: role_credentials)
bucket_name = nil


if (ARGV.length < 1)
	operation = 'help' 
else
	operation = ARGV[0]
end

# The operation to perform on the bucket
 # default
bucket_name = ARGV[1] if (ARGV.length > 1)

# The file name to use with 'upload'
file_name = nil
file_name = ARGV[2] if (ARGV.length > 2)

# The new name to use with 'rename'
new_name = nil
new_name = ARGV[3] if (ARGV.length > 3)

def add_item_to_table(dynamodb_client, table_item)
  dynamodb_client.put_item(table_item)
  puts "Added '#{table_item[:item][:pk]} " \
    "(#{table_item[:item][:sk]})'."
rescue StandardError => e
  puts "Error adding '#{table_item[:item][:pk]} " \
    "(#{table_item[:item][:sk]})': #{e.message}"
end

def run_me(obj_type, genre_name: nil, artist_name: nil, album_name: nil, song_name: nil, song_path: nil)
  region = 'us-east-1'
  table_name = 'music-table-dev'
  genre = genre_name
  artist = artist_name
  album = album_name
  song = song_name
  song_path = song_path

  # To use the downloadable version of Amazon DynamoDB,
  # uncomment the endpoint statement.
  Aws.config.update(
    # endpoint: 'http://localhost:8000',
    region: region
  )

  dynamodb_client = Aws::DynamoDB::Client.new

  if obj_type == "songByAlbum"
		item = {
			pk: "album##{album}",
			sk: "song##{song}",
			info: {
			genre: genre,
			artist: artist,
			albums: album,
			song: song_path
			}
		}
	elsif obj_type == "songByName"
		item = {
			pk: "song",
			sk: "song##{song}",
			info: {
			genre: genre,
			artist: artist,
			albums: album,
			song: song_path
			}
		}
  elsif obj_type == "album"
		item = {
			pk: "artist##{artist}",
			sk: "album##{album}",
			info: {
			genre: genre,
			artist: artist,
			albums: album,
			song: song
			}
		}
  elsif obj_type == "artist"
		item = {
			pk: "genre##{genre}",
			sk: "artist##{artist}",
			info: {
			genre: genre,
			artist: artist,
			albums: album,
			song: song
			}
		}
  elsif obj_type == "genre"
		item = {
			pk: "genre",
			sk: "genre##{genre}",
			info: {
				genre: genre
			}
		}
  end

  table_item = {
    table_name: table_name,
    item: item
  }

  puts "Adding #{obj_type}'#{item[:pk]} (#{item[:sk]})' " \
    "to table '#{table_name}'..."
  add_item_to_table(dynamodb_client, table_item)
end

case operation
when 'upload_song'
  if file_name == nil
    puts "You must enter the name of the file to upload to S3!"
    exit
  end
	run_me("songByName", song_name: file_name)

  puts "Uploading: #{file_name}..."
 	s3.put_object({
	  bucket: bucket_name, 
	  key: file_name,
	  body: file_name
	})
	puts "Upload complete."

when 'upload_album'
	if file_name == nil
    puts "You must enter the name of the file to upload to S3!"
    exit
  end

	songs = Dir.entries(file_name)
	run_me("album", album_name: file_name, song_name: songs)

 	songs.each do |song|
 		next if song == '.' or song == '..'
 		puts "Uploading: #{file_name}#{song}..."
 		run_me("songByName", album_name: file_name, song_name: song)
 		run_me("songByAlbum", album_name: file_name, song_name: song)
 		s3.put_object({
		  bucket: bucket_name, 
		  key: "#{file_name}#{song}",
		  body: file_name
		})
 	end
 	puts "Upload complete."

when 'upload_artist'
	if file_name == nil
    puts "You must enter the name of the file to upload to S3!"
    exit
  end
  puts "what is the song Genre Name?"
	genre_name = STDIN.gets.chomp

  artist = Pathname(file_name)
  run_me("artist",artist_name: file_name, genre_name: genre_name)
  run_me("genre",artist_name: file_name, genre_name: genre_name)
  albums = artist.children()

	albums.each do |album|
		next if !album.directory?
		songs = Dir.each_child(album)
		album_path = album.to_s.split('/')
		run_me("album", artist_name: file_name, album_name: album_path[1])
		Dir.each_child(album) do |song|
			run_me("songByAlbum", artist_name: file_name, album_name: album_path[1], song_path: "#{album}/#{song}", song_name: song.to_s)
			run_me("songByName", artist_name: file_name, album_name: album_path[1], song_path: "#{album}/#{song}", song_name: song.to_s)
      puts "Uploading: #{album}/#{song}..."
      s3.put_object( bucket: bucket_name, key: "#{album}/#{song}", body: "#{album}/#{song}")
	  end
	end
	puts "Upload complete."

when 'rename'
  if file_name == nil
    puts "You must enter the name of the file to rename!"
    exit
  end

 	s3.copy_object({
 		bucket: bucket_name, 
	  copy_source: "/#{bucket_name}/#{file_name}", 
	  key: new_name,
 	})
 	s3.delete_object({
	  bucket: bucket_name, 
	  key: file_name, 
	})
	puts "Rename complete."


when 'list'
	objects = s3.list_objects_v2({
		bucket: bucket_name
	}).contents

	if objects.length > 0
		objects.each do |object|
			puts object.key
		end
	end

when 'help'
	def help_message
	 <<~HELP
      To List the contents of the bucket: ruby app.rb list [bucket name]
      To Rename a file in the bucket:     ruby app.rb rename [bucket name] [file name] [new name]
      To Upload a Song:                   ruby app.rb upload_song [bucket name] [file name]
      To Upload an Album:                 ruby app.rb upload_album [bucket name] [file name]
      To Upload an Artist:                ruby app.rb upload_artist [bucket name] [file name]
    HELP
  end
  puts help_message
when 'put'
	puts "what is the upload type?"
	obj_type = STDIN.gets.chomp

	puts "What is the Song Name?"
	song_name = STDIN.gets.chomp

	puts "what is the song Album Name?"
	album_name = STDIN.gets.chomp

	puts "what is the song Artist Name?"
	artist_name = STDIN.gets.chomp

	puts "what is the song Genre Name?"
	genre_name = STDIN.gets.chomp

	run_me(obj_type, genre_name, artist_name, album_name, song_name)
else
  puts "Unknown operation: '%s'!" % operation
end

