module MIPPeR
  class LinExpr
    attr_reader :terms

    def initialize(terms = {})
      @terms = terms
    end

    # Add two {LinExpr}s
    def +(other)
      case other
      when LinExpr
        # Add the terms of two expressions
        # For now, this assumes variables are not duplicated
        LinExpr.new(@terms.merge(other.terms) { |_, c1, c2| c1 + c2 })
      when Variable
        # Add the variable to the expression
        self + other * 1.0
      else
        fail TypeError
      end
    end

    # Add terms from the other expression to this one
    def add(other)
      case other
      when LinExpr
        @terms.merge!(other.terms) { |_, c1, c2| c1 + c2 }
      when Variable
        if @terms.key? other
          @terms[other] += 1.0
        else
          @terms[other] = 1.0
        end
      else
        fail TypeError
      end

      self
    end

    # Produce a string representing the expression
    def inspect
      @terms.map do |var, coeff|
        # Skip if the coefficient is zero or the value is zero
        value = var.value
        next if coeff == 0 || value == 0 || value == false

        coeff == 1 ? var.name : "#{var.name} * #{coeff}"
      end.compact.join(' + ')
    end
  end
end
