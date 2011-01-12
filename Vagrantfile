Vagrant::Config.run do |config|
  config.vm.box_url = "http://files.vagrantup.com/lucid64.box"
  config.vm.box = "lucid64"
  config.vm.provisioner = :chef_solo
  config.vm.network "10.0.6.2"
  config.vm.forward_port("mongodb", 27017, 27017)
  config.chef.cookbooks_path = "cookbooks"
  config.chef.add_recipe "mongodb"
end
