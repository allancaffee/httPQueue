package "build-essential"
package "libevent-dev"
package "nginx"
package "python-dev"
package "python-setuptools"

easy_install_package "flask"
easy_install_package "gevent"
easy_install_package "gunicorn"

# This doesn't actually fit here, but it needs the python-setuptools...
easy_install_package "mongokit"

cookbook_file "/etc/nginx/nginx.conf"

service "nginx" do
  action [ :enable, :start, :restart ]
end
