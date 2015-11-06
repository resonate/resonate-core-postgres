Vagrant.configure(2) do |config|
    config.vm.provider :virtualbox do |vb|
        vb.memory = 1024
        vb.cpus = 2
    end

    config.vm.box = "ubuntu/precise64"

    config.vm.provider :virtualbox do |vb|
        vb.name = "resonate-core-ubuntu-12.04"
    end

    # Provision the VM
    config.vm.provision "shell", inline: <<-SHELL
        # Install db packages
        echo 'deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main' > /etc/apt/sources.list.d/pgdg.list
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
        apt-get update

        # Install ant and ant-contrib
        apt-get install -y openjdk-7-jdk
        apt-get install -y ant
        apt-get install -y ant-contrib
        apt-get install -y git

        # Install and start db
        apt-get install -y postgresql-9.3
        apt-get install -y postgresql-contrib-9.3
        pg_createcluster 9.3 main
        pg_ctlcluster 9.3 main start

        # Create vagrant user to do builds
        sudo -u postgres psql -c "create user vagrant with password 'vagrant' superuser" postgres

        # Create a data tablespace (never create a tablespace in $PGDATA on a production server!)
        sudo -u postgres mkdir /var/lib/postgresql/9.3/main/ts_data_n01
        sudo -u postgres psql -c "create tablespace ts_data_n01 location '/var/lib/postgresql/9.3/main/ts_data_n01'" postgres
    SHELL

  # Share the resonate folder
  config.vm.synced_folder ".", "/resonate"
end
