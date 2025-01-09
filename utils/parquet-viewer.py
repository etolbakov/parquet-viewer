#!/usr/bin/env python3
# encoding: utf-8

from http.server import HTTPServer, SimpleHTTPRequestHandler
import argparse
import os


class CORSRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, file_path=None, **kwargs):
        self.file_path = file_path
        super().__init__(*args, **kwargs)

    def end_headers(self):
        # Add CORS headers to allow all origins
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        super().end_headers()

    def do_OPTIONS(self):
        # Handle preflight requests
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        expected_path = '/' + os.path.basename(self.file_path)
        if self.path != expected_path:
            self.send_error(404, f"File not found. Please use {expected_path}")
            return

        try:
            file_size = os.path.getsize(self.file_path)
            range_header = self.headers.get('Range')

            if range_header:
                try:
                    range_match = range_header.replace('bytes=', '').split('-')
                    range_start = int(range_match[0])
                    range_end = int(range_match[1]) if range_match[1] else file_size - 1
                    
                    if range_start >= file_size:
                        self.send_error(416, 'Requested range not satisfiable')
                        return
                    
                    self.send_response(206)
                    self.send_header('Content-Range', f'bytes {range_start}-{range_end}/{file_size}')
                    content_length = range_end - range_start + 1
                except ValueError:
                    self.send_error(400, 'Invalid range header')
                    return
            else:
                self.send_response(200)
                range_start = 0
                content_length = file_size

            self.send_header('Accept-Ranges', 'bytes')
            self.send_header('Content-Length', str(content_length))
            self.send_header('Content-type', 'application/vnd.apache.parquet')
            self.send_header('Content-Disposition', f'attachment; filename="{os.path.basename(self.file_path)}"')
            self.end_headers()

            with open(self.file_path, 'rb') as f:
                if range_header:
                    f.seek(range_start)
                    self.wfile.write(f.read(content_length))
                else:
                    self.wfile.write(f.read())

        except FileNotFoundError:
            self.send_error(404, "File not found on server")
        except Exception as e:
            self.send_error(500, f"Internal server error: {str(e)}")

    def do_HEAD(self):
        expected_path = '/' + os.path.basename(self.file_path)
        if self.path != expected_path:
            self.send_error(404, f"File not found. Please use {expected_path}")
            return

        try:
            file_size = os.path.getsize(self.file_path)
            self.send_response(200)
            self.send_header('Accept-Ranges', 'bytes')
            self.send_header('Content-Length', str(file_size))
            self.send_header('Content-type', 'application/vnd.apache.parquet')
            self.send_header('Content-Disposition', f'attachment; filename="{os.path.basename(self.file_path)}"')
            self.end_headers()
        except FileNotFoundError:
            self.send_error(404, "File not found on server")
        except Exception as e:
            self.send_error(500, f"Internal server error: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Start a CORS-enabled HTTP server')
    parser.add_argument('-p', '--port', type=int, default=8003,
                       help='Port to run the server on (default: 8003)')
    parser.add_argument('-f', '--file', type=str, required=True,
                       help='Path to the file to serve')
    args = parser.parse_args()

    file_name = os.path.basename(args.file)
    handler = lambda *handler_args: CORSRequestHandler(*handler_args, file_path=args.file)
    httpd = HTTPServer(('127.0.0.1', args.port), handler)
    print(f'Open in your browser: https://parquet-viewer.xiangpeng.systems/?url=http://127.0.0.1:{args.port}/{file_name}')
    httpd.serve_forever()


if __name__ == '__main__':
    main()
