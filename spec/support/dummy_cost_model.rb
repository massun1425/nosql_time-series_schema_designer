module NoSE
  module Cost
    RSpec.shared_examples 'dummy cost model' do
      let(:cost_model) do
        # Simple cost model which just counts the number of indexes
        class DummyCost < NoSE::Cost::Cost
          include Subtype

          def index_lookup_cost(_step)
            2
          end

          def insert_cost(_step)
            0.1
          end

          def delete_cost(_step)
            0.1
          end

          def prepare_insert_cost(_step)
            0
          end

          def prepare_delete_cost(_step)
            0
          end
        end

        DummyCost.new
      end
    end
  end
end
