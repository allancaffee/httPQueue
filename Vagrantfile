Vagrant::Config.run do |config|
  config.vm.box_url = "http://files.vagrantup.com/lucid64.box"
  config.vm.box = "lucid64"
  config.vm.network "10.0.6.2"
  config.vm.forward_port("mongodb", 27017, 27017)
  config.vm.forward_port("gunicorn", 8000, 8000)
  config.vm.provision :chef_solo do |chef|
    chef.cookbooks_path = "cookbooks"
    chef.add_recipe "mongodb"
    chef.add_recipe "gunicorn"
  end
end
