#
# Cookbook Name:: mongodb
# Recipe:: default
#
# Copyright 2011, AWeber Inc
#
# All rights reserved - Do Not Redistribute


cookbook_file "/etc/apt/sources.list.d/10gen.list" do
    source "10gen.list"
end

execute "sudo apt-key adv --keyserver keyserver.ubuntu.com --recv 7F0CEB10; mkdir -p /etc/chef/created; touch /etc/chef/created/sources.list" do
    creates "/etc/chef/created/sources.list"
    action :run
end

execute "apt-get update; mkdir -p /etc/chef/created; touch /etc/chef/created/apt.list" do
    creates "/etc/chef/created/apt.list"
    action :run
end

package "mongodb-stable" do
    action :install
end
