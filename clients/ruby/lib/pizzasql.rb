require 'net/http'
require 'uri'
require 'json'

module PizzaSQL
  class Client
    attr_reader :base_url, :db_name, :api_key

    # Creates a new PizzaSQL client connection
    #
    # @param uri [String] Database URI (e.g., 'http://localhost:8080/mydb')
    # @param api_key [String, nil] Optional API key for authentication
    # @return [Client] A new client instance
    def initialize(uri, api_key = nil)
      parsed_uri = URI.parse(uri)

      # Extract database name from path
      path = parsed_uri.path.strip.delete_prefix('/')
      raise ArgumentError, 'Database name not found in URI path' if path.empty?

      # Split path to get database name (last segment)
      path_parts = path.split('/')
      @db_name = path_parts.last

      # Reconstruct base URL without the database path
      @base_url = "#{parsed_uri.scheme}://#{parsed_uri.host}:#{parsed_uri.port}"
      @api_key = api_key
    end

    # Executes a SQL query and returns the results
    #
    # @param query [String] SQL query string
    # @return [Array<Hash>] Array of rows (each row is a hash)
    # @raise [RuntimeError] if the query fails
    def sql(query)
      uri = URI.parse("#{@base_url}/#{@db_name}/query")

      request = Net::HTTP::Post.new(uri)
      request['Content-Type'] = 'application/json'
      request['Authorization'] = "Bearer #{@api_key}" if @api_key
      request.body = { query: query }.to_json

      response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
        http.request(request)
      end

      unless response.is_a?(Net::HTTPSuccess)
        raise "Request failed with status #{response.code}: #{response.body}"
      end

      result = JSON.parse(response.body)
      result['rows'] || []
    end

    # Exports database or table data
    #
    # @param table [String, nil] Table name (nil for entire database)
    # @param format [String] Export format: 'sql' or 'csv'
    # @return [String] Exported data
    # @raise [RuntimeError] if the export fails
    def export(table: nil, format: 'sql')
      params = {}
      params['table'] = table if table
      params['format'] = format if format

      query_string = URI.encode_www_form(params)
      uri = URI.parse("#{@base_url}/#{@db_name}/export?#{query_string}")

      request = Net::HTTP::Get.new(uri)
      request['Authorization'] = "Bearer #{@api_key}" if @api_key

      response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
        http.request(request)
      end

      unless response.is_a?(Net::HTTPSuccess)
        raise "Export failed with status #{response.code}: #{response.body}"
      end

      response.body
    end

    # Imports data into the database
    #
    # @param data [String] Data to import
    # @param format [String] Import format: 'sql' or 'csv'
    # @param create_table [Boolean] Create table if it doesn't exist (CSV only)
    # @return [void]
    # @raise [RuntimeError] if the import fails
    def import(data, format: 'sql', create_table: false)
      params = {}
      params['format'] = format if format
      params['create_table'] = 'true' if create_table

      query_string = URI.encode_www_form(params)
      uri = URI.parse("#{@base_url}/#{@db_name}/import?#{query_string}")

      request = Net::HTTP::Post.new(uri)
      request['Content-Type'] = 'application/octet-stream'
      request['Authorization'] = "Bearer #{@api_key}" if @api_key
      request.body = data

      response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
        http.request(request)
      end

      unless response.is_a?(Net::HTTPSuccess)
        raise "Import failed with status #{response.code}: #{response.body}"
      end

      nil
    end
  end

  # Module-level convenience method to create a new client connection
  #
  # @param uri [String] Database URI
  # @param api_key [String, nil] Optional API key
  # @return [Client] A new client instance
  def self.connect(uri, api_key = nil)
    Client.new(uri, api_key)
  end
end
