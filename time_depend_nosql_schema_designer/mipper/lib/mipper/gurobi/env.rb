module MIPPeR
  module Gurobi
    class Environment
      attr_reader :ptr

      def initialize
        # Create a new environment object
        @ptr = FFI::MemoryPointer.new :pointer
        Gurobi.GRBloadenv @ptr, nil
        @ptr = @ptr.read_pointer

        # Disable output
        Gurobi.GRBsetintparam @ptr, Gurobi::GRB_INT_PAR_OUTPUTFLAG, 0

        # strict gap
        Gurobi.GRBsetintparam @ptr, Gurobi::GRB_DBL_ATTR_MIPGAP, 0
        Gurobi.GRBsetintparam @ptr, Gurobi::GRB_DBL_PAR_MIPGAPABS, 0

        ## strict Numeric Focus
        Gurobi.GRBsetintparam @ptr, Gurobi::GRB_INT_PAR_NUMERICFOCUS, 3

        # Ensure the environment is freed
        ObjectSpace.define_finalizer self, self.class.finalize(@ptr)
      end

      # Free the environment
      def self.finalize(ptr)
        proc { Gurobi.GRBfreeenv ptr }
      end
    end
  end
end
