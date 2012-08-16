
require 'tempfile'

Puppet::Type.newtype(:flume_node) do

  @doc = "Puppet type for configuring Flume nodes with the Master"

  newparam(:name) do
    desc "logical node name"
  end

  newparam(:source) do
    desc "source config"
  end

  newparam(:sink) do
    desc "sink config"
  end

  newparam(:master) do
    desc "master hostname"
  end

  newparam(:flow) do
    desc "Flow ID"
    defaultto "default-flow"
  end

  newparam(:map) do
    desc "Mapping to use"
    defaultto {
      nil
    }
  end

  newproperty(:ensure) do
    desc "Whether the resource is in sync or not."

    defaultto :insync

    def retrieve
      map = resource[:map]
      if !map.nil? then
        node = map
      else
        node = resource[:name]
      end
      `flume shell -q -c #{resource[:master]} -e getconfigs 2>/dev/null | grep #{node} | grep -v null`
      ($? == 0 ? :insync : :outofsync)
    end

    newvalue :outofsync do
      master = resource[:master]
      name = resource[:name]
      map = resource[:map]
      
      if !map.nil? then
        mapping = <<-EOF
exec unmap #{name} #{map}
exec unconfig #{map}
exec decommission #{map}
exec purge #{map}
EOF
      end
      conf = <<-EOF
connect #{master}
exec unconfig #{name}
exec decommission #{name}
exec purge #{name}
EOF

      Tempfile.open("flume-") do |tempfile|
        tempfile.write(conf)
        if defined? mapping
          tempfile.write(mapping)
        end
        tempfile.close
        `cat #{tempfile.path} | flume shell -q 2>/dev/null`
      end
    end

    newvalue :insync do

      master = resource[:master]
      name = resource[:name]
      source = resource[:source]
      sink = resource[:sink]
      flow = resource[:flow]
      map = resource[:map]

      if sink.kind_of? Hash then
        # support hashes of the format:
        # { sinkType => [ array of nodes ] }
        # e.g.
        # { agentE2EChain => [ "flume1.example.com:35853", "flume2.example.com:35853" ] }
        sink = sink.keys.first + "( " + sink.values.first.shuffle.map{ |s| "\"#{s}\"" }.join(", ") + " )"
      end

      if !map.nil? then
        node = map
        conf = <<-EOF
connect #{master}
exec unmap #{name} #{node}
exec unconfig #{node}
exec decommission #{node}
exec purge #{node}
exec decommission #{name}
exec purge #{name}
exec map #{name} #{node}
exec config #{node} #{flow} '#{source}' '#{sink}'
exec refresh #{node}
EOF
      else
        node = name
        conf = <<-EOF
connect #{master}
exec unconfig #{name}
exec decommission #{name}
exec purge #{name}
exec config #{node} #{flow} '#{source}' '#{sink}'
exec refresh #{name}
EOF
      end

      Tempfile.open("flume-") do |tempfile|
        tempfile.write(conf)
        tempfile.close
        `cat #{tempfile.path} | flume shell -q 2>/dev/null`
      end

    end

  end

end
