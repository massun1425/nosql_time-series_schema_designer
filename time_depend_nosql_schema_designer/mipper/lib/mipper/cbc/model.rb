require 'zlib'

module MIPPeR
  # A linear programming model using the COIN-OR solver
  class CbcModel < Model
    attr_reader :ptr

    def initialize
      fail unless MIPPeR.const_defined?(:Cbc)

      super

      @var_count = 0
      @constr_count = 0

      # Construct a new model
      @ptr = new_model
    end

    # Write the model to a file in MPS format
    def write_mps(filename)
      # Make a new model and ensure everything is added
      old_ptr = @ptr
      @ptr = new_model
      parent_update

      Cbc.Cbc_writeMps @ptr, filename.chomp('.mps')
      contents = Zlib::GzipReader.open(filename + '.gz').read
      File.delete(filename + '.gz')
      File.open(filename, 'w').write contents

      # Reset to the original model
      @ptr = old_ptr
      reset_model
    end

    alias_method :parent_update, :update

    # Avoid doing anything here. Updating multiple times will
    # break the model so we defer to #solve.
    def update
    end

    # Set the sense of the model
    def sense=(sense)
      @sense = sense
      sense = sense == :min ? 1 : -1
      Cbc.Cbc_setObjSense @ptr, sense
    end

    # Optimize the model
    def optimize
      # Ensure pending variables and constraints are added
      parent_update

      # Run the solver and save the status for later
      Cbc.Cbc_solve @ptr
      fail if Cbc.Cbc_status(@ptr) != 0

      save_solution

      @ptr = new_model
      reset_model
    end

    # Set the bounds of a variable in the model
    def set_variable_bounds(var_index, lb, ub, force = false)
      # This is a bit of a hack so that we don't try to set
      # the variable bounds before they get added to the model
      return unless force

      Cbc.Cbc_setColLower @ptr, var_index, lb
      Cbc.Cbc_setColUpper @ptr, var_index, ub
    end

    protected

    # Add multiple variables to the model simultaneously
    def add_variables(vars)
      # Store all the variables in the model
      # Most of the work will be done when we add the constraints
      vars.each do |var|
        var.model = self
        var.index = @variables.count
        @variables << var
      end
    end

    # Add multiple constraints at once
    def add_constraints(constrs)
      start, index, value = build_constraint_matrix constrs
      start_buffer = build_pointer_array start, :int
      index_buffer = build_pointer_array index, :int
      value_buffer = build_pointer_array value, :double

      Cbc.Cbc_loadProblem @ptr, @variables.length, constrs.length,
                          start_buffer, index_buffer, value_buffer,
                          nil, nil, nil, nil, nil

      store_model constrs, @variables
    end

    private

    # Construct a new model object
    def new_model
      ptr = FFI::AutoPointer.new Cbc.Cbc_newModel,
                                 Cbc.method(:Cbc_deleteModel)

      # Older versions of COIN-OR do not support setting the log level via
      # the C interface in which case setParameter will not be defined
      Cbc.Cbc_setParameter ptr, 'logLevel', '0' \
        if Cbc.respond_to?(:Cbc_setParameter)

      ptr
    end

    # Store the index which will be used for each constraint
    def store_constraint_indexes(constrs)
      constrs.each do |constr|
        constr.model = self
        constr.index = @constr_count
        @constr_count += 1
      end
    end

    # Build a constraint matrix for the currently existing variables
    def build_constraint_matrix(constrs)
      store_constraint_indexes constrs

      # Construct a matrix of non-zero values in CSC format
      start = []
      index = []
      value = []
      col_start = 0
      @variables.each do |var|
        # Mark the start of this column
        start << col_start

        var.constraints.each do |constr|
          col_start += 1
          index << constr.index
          value << constr.expression.terms[var]
        end
      end
      start << col_start

      [start, index, value]
    end

    # Store all data for the model
    def store_model(constrs, vars)
      # Store all constraints
      constrs.each do |constr|
        store_constraint constr
        @constraints << constr
      end

      # We store variables now since they didn't exist earlier
      vars.each_with_index do |var, i|
        var.index = i
        var.model = self
        store_variable var
      end
    end

    # Save the solution to the model for access later
    def save_solution
      # Check and store the model status
      if Cbc.Cbc_isProvenOptimal(@ptr) == 1
        status = :optimized
      elsif Cbc.Cbc_isProvenInfeasible(@ptr) == 1 ||
            Cbc.Cbc_isContinuousUnbounded(@ptr) == 1
        status = :invalid
      else
        status = :unknown
      end

      if status == :optimized
        objective_value = Cbc.Cbc_getObjValue @ptr
        dblptr = Cbc.Cbc_getColSolution @ptr
        variable_values = dblptr.read_array_of_double(@variables.length)
      else
        objective_value = nil
        variable_values = []
      end

      @solution = Solution.new status, objective_value, variable_values
    end

    # Reset the internal state of the model so we can reuse it
    def reset_model
      @var_count = 0
      @constr_count = 0
      @pending_variables = @variables
      @pending_constraints = @constraints
      @variables = []
      @constraints = []
    end

    # Save the constraint to the model and update the constraint pointers
    def store_constraint(constr)
      # Update the constraint to track the index in the model
      constr.model = self

      # Set constraint properties
      Cbc.Cbc_setRowName(@ptr, constr.index, constr.name) unless constr.name.nil?
      store_constraint_bounds constr.index, constr.sense, constr.rhs
    end

    # Store the bounds for a given constraint
    def store_constraint_bounds(index, sense, rhs)
      case sense
      when :==
        lb = ub = rhs
      when :>=
        lb = rhs
        ub = Float::INFINITY
      when :<=
        lb = -Float::INFINITY
        ub = rhs
      end

      Cbc.Cbc_setRowLower @ptr, index, lb
      Cbc.Cbc_setRowUpper @ptr, index, ub
    end

    # Set the properties of a variable in the model
    def store_variable(var)
      # Force the correct bounds since we can't explicitly specify binary
      if var.type == :binary
        var.instance_variable_set(:@lower_bound, 0)
        var.instance_variable_set(:@upper_bound, 1)
      end
      set_variable_bounds var.index, var.lower_bound, var.upper_bound, true

      Cbc.Cbc_setObjCoeff @ptr, var.index, var.coefficient
      Cbc.Cbc_setColName(@ptr, var.index, var.name) unless var.name.nil?
      set_variable_type var.index, var.type
    end

    # Set the type of a variable
    def set_variable_type(index, type)
      case type
      when :continuous
        Cbc.Cbc_setContinuous @ptr, index
      when :integer, :binary
        Cbc.Cbc_setInteger @ptr, index
      else
        fail :type
      end
    end
  end
end
