using Distributions
using Sockets
using Base64
using Plots
using Flux
using SQLite

# define
gr()
default(legend = false)

# types and structs
abstract type SynapsTemplate end
abstract type NodeTemplate end
struct StaticSynapsTemplate <: SynapsTemplate
    init_position::Array{Normal}
    threshold::Real
    init_value::Real
end
struct StaticUnisynNodeTemplate <: NodeTemplate
    init_position::Array{Normal}
    n_in_syn::Integer
    n_out_syn::Integer
    syn_template::SynapsTemplate
end
mutable struct Synaps
    position::Array{Real}
    threshold::Real
    current_value::Real
    previous_value::Real

    Synaps(init_pos::Array{Real}, init_thr::Real) = new(init_pos, init_thr, 0., 0.)
    Synaps(t::StaticSynapsTemplate) = new([rand(in_p) for in_p in t.init_position], t.threshold, t.init_value, t.init_value)
end
mutable struct Node

    position::Array{Real}

    current_closest_nodes::Array{Node}
    current_related_input_nodes::Array{Node}
    current_related_output_nodes::Array{Node}

    previous_closest_nodes::Array{Node}
    previous_related_input_nodes::Array{Node}
    previous_related_output_nodes::Array{Node}

    all_synapses::Array{Synaps}
    input_synapses::Array{Synaps}
    output_synapses::Array{Synaps}

    function Node(init_position::Array{Real}, n_in_syn::Integer, n_out_syn::Integer, syn_init_thr::Real)
        in_syns = [Synaps(syn_init_thr) for _ in 1:n_in_syn]
        out_syns = [Synaps(syn_init_thr) for _ in 1:n_out_syn]
        return new(init_position, Array{Node}([]), Array{Node}([]), Array{Node}([]), Array{Node}([]), Array{Node}([]), Array{Node}([]), [in_syns..., out_syns...], in_syns, out_syns)
    end
    function Node(t::StaticUnisynNodeTemplate)
        in_syns = [Synaps(t.syn_template) for _ in 1:t.n_in_syn]
        out_syns = [Synaps(t.syn_template) for _ in 1:t.n_out_syn]
        return new([rand(in_p) for in_p in t.init_position], Array{Node}([]), Array{Node}([]), Array{Node}([]), Array{Node}([]), Array{Node}([]), Array{Node}([]), [in_syns..., out_syns...], in_syns, out_syns)
    end
end



# utils
mean(x::Array) = sum(x) / length(x)
rbfk(x1::Array, x2::Array, σ::Real) = ℯ ^ -((mean((x1 .- x2) .^ 2) / σ))
mse(x1::Array, x2::Array) = sum((x1 .- x2) .^ 2)
rmse(x1::Array, x2::Array) = sqrt(mse(x1, x2))
related_node_distance_oi(n1::Node, n2::Node) = mean([rmse(s1.position, s2.position) for (s1, s2) in Iterators.product(n1.output_synapses, n2.input_synapses)])
related_node_distance_io(n1::Node, n2::Node) = mean([rmse(s1.position, s2.position) for (s1, s2) in Iterators.product(n1.input_synapses, n2.output_synapses)])
closest_node_distance(n1::Node, n2::Node) = rmse(n1.position, n2.position)


# hyperparams
init_n_nodes = 5
input_dim = 1
output_dim = 1
n_closest_nodes = 3
n_related_input_nodes = 3
n_related_output_nodes = 3
intra_node_cohesion_factor = 1.
node_synapses_cohesion_factor = 1.
synaptic_sensitivity_factor = 1.
input_strength = 1.
proximity_nodes_update_interval = 5


# network initialization
base_syn_template = StaticSynapsTemplate([Normal(0, 1), Normal(0, 1)], 0.5, 0.)
base_node_template = StaticUnisynNodeTemplate([Normal(0, 1), Normal(0, 1)], 2, 1, base_syn_template)
all_nodes = [Node(base_node_template) for _ in 1:init_n_nodes]
input_nodes = [all_nodes[i] for i in 1:input_dim]
output_nodes = [all_nodes[end-i+1] for i in 1:output_dim]


# asign the related_input_nodes & closest_nodes
for n in all_nodes
    current_closest_nodes = sort(all_nodes, by = nn-> nn != n ? closest_node_distance(n, nn) : NaN)
    current_related_input_nodes = sort(all_nodes, by = nn-> (nn != n) & !(nn in input_nodes) ? related_node_distance_oi(n, nn) : NaN)
    current_related_output_nodes = sort(all_nodes, by = nn-> (nn != n) & !(nn in output_nodes) ? related_node_distance_io(n, nn) : NaN)

    n.current_closest_nodes = current_closest_nodes[1:n_closest_nodes]
    n.current_related_input_nodes = current_related_input_nodes[1:n_related_input_nodes]
    n.current_related_output_nodes = current_related_output_nodes[1:n_related_output_nodes]
end


# data initialization
X = [[0, 0, 1], [0, 0, 1], [0, 1, 0], [0, 1, 0]]
Y = [[0, 0, 1], [0, 0, 1], [0, 1, 0], [0, 1, 0]]


# train
for (e, (x, y)) in enumerate(zip(X, Y))

    intra_node_cohesion_loss = []
    node_synapses_cohesion_loss = []

    # write input
    for (i, n) in enumerate(input_nodes)
        for s in n.output_synapses
            s.current_value = x[i]
        end
    end


    # update temporal values
    for n in all_nodes

        n.previous_closest_nodes = n.current_closest_nodes
        n.previous_related_input_nodes = n.current_related_input_nodes
        n.previous_related_output_nodes = n.current_related_output_nodes

        for s in n.all_synapses
            s.previous_value = s.current_value
            s.current_value = 0.
        end
    end


    # pass network
    for n in all_nodes

        # keep nodes evenly distributed
        for cn in n.previous_closest_nodes
            append!(intra_node_cohesion_loss, (intra_node_cohesion_factor - mse(n.position, cn.position))^2)
        end

        # keep neuron and synapses close together
        for s in n.all_synapses
            append!(node_synapses_cohesion_loss, (node_synapses_cohesion_factor - mse(n.position, s.position))^2)
        end


        activation_total = 0.

        # ! activate synapses based on related nodes and threshold
        for is in n.input_synapses
            activation_total += is.previous_value

            activational_values = []
            for rn in n.previous_related_input_nodes
                for os in rn.output_synapses
                    dist_v = rbfk(os.position, is.position, synaptic_sensitivity_factor)
                    act_v = os.previous_value * dist_v
                    append!(activational_values, act_v)
                end
            end

            is.current_value = mean(activational_values)
        end

        # ! propergate activation to output nodes and threshold for polarity
        for os in n.output_synapses
            os.current_value = elu(activation_total - os.threshold)
        end


        # every 1:M steps: reasign the related_nodes & closest_nodes
        if e % proximity_nodes_update_interval == 0
            for nn in all_nodes

                if n != nn

                    # closest node
                    new_cn_dist = closest_node_distance(n, nn)
                    for (ccn_i, ccn) in enumerate(n.current_closest_nodes)
                        old_cn_dist = closest_node_distance(n, ccn)

                        if new_cn_dist < old_cn_dist
                            n.current_closest_nodes[ccn_i] = nn
                            break
                        end
                    end

                    # related input node
                    if !(nn in input_nodes)
                        new_rin_dist = related_node_distance_oi(n, nn)
                        for (crin_i, crin) in enumerate(n.current_related_input_nodes)
                            old_rin_dist = related_node_distance_oi(n, crin)

                            if new_rin_dist < old_rin_dist
                                n.current_related_input_nodes[crin_i] = nn
                                break
                            end
                        end
                    end

                    # related output node
                    if !(nn in output_nodes)
                        new_ron_dist = related_node_distance_io(n, nn)
                        for (cron_i, cron) in enumerate(n.current_related_output_nodes)
                            old_ron_dist = related_node_distance_io(n, cron)

                            if new_ron_dist < old_ron_dist
                                n.current_related_output_nodes[cron_i] = nn
                                break
                            end
                        end
                    end
                end
            end
        end

    end


    # read output
    # all_network_values = collect(Base.Flatten([[s.current_value for s in n.all_synapses] for n in all_nodes]))
    output_values = collect(Base.Flatten([[s.current_value for s in n.output_synapses] for n in output_nodes]))

    # println(all_network_values)

    # do optimization step
    intra_node_cohesion_loss_total = mean(intra_node_cohesion_loss)
    node_synapses_cohesion_loss_total = mean(node_synapses_cohesion_loss)

    rec! = (n, s) -> begin
        if n in input_nodes
            return
        else
            for i_n in n.input_nodes
                for ro_n in n.related_output_nodes
                    # TODO
                end
            end
        end
    end

    for (i, n) in enumerate(output_nodes)
        TP = y[i] > 0.5 & n.output_synapses[1] > 0.5 # pull best & normalise rest (norm to 0)
        TN = y[i] < 0.5 & n.output_synapses[1] < 0.5 # push best & normalise rest (norm to 0)
        FP = y[i] < 0.5 & n.output_synapses[1] > 0.5 # push best & denormalise rest
        FN = y[i] > 0.5 & n.output_synapses[1] < 0.5 # pull best & denormalise rest


        if TP | FN
        else if FP | TN
            for n in n.related_output_nodes
            end
        end
    end


    # convergence_speed = 0.02
    # stabalisation_speed = 0.02
    # temperature = 0.0

    # for n in all_nodes
    #     closest_node_relative = n.current_related_input_nodes[1]
    #     input_synaps_mean = mean([s.position for s in n.input_synapses])

    #     for s in n.input_synapses
    #         closest_synaps_relative = sort(closest_node_relative.output_synapses, by = x -> rmse(x.position, s.position))[1]
    #         convergence_vector = convergence_speed * (closest_synaps_relative.position - s.position)
    #         stabalisation_vector = stabalisation_speed * ((s.position - input_synaps_mean) + (n.position - s.position))
    #         temperature_vector = temperature * (rand(2) .- 0.5)
    #         s.position = s.position + temperature_vector + convergence_vector + stabalisation_vector
    #     end
    # end


    # visualize

    println("---------------------------------------------------------")
    println("intra_node_cohesion_loss_total:    ", intra_node_cohesion_loss_total)
    println("node_synapses_cohesion_loss_total: ", node_synapses_cohesion_loss_total)


    l = @layout [a{0.8h}; b]
    p = plot(size = (1500, 1500), tickfontsize = 15, layout = l)

    plot!(p[2], [i for i in 1:length(intra_node_cohesion_loss)], intra_node_cohesion_loss)

    for n in all_nodes
        scatter!(p[1], [n.position[1]], [n.position[2]], markersize=8, markercolor=:red)

        for s in n.input_synapses
            plot!(p[1], [n.position[1], s.position[1]], [n.position[2], s.position[2]], linewidth=5, c=:black)
        end
        for s in n.output_synapses
            plot!(p[1], [n.position[1], s.position[1]], [n.position[2], s.position[2]], linewidth=1, c=:green)
        end
    end

    display(p)

end




# # -------------------------------------------------------------

# mutable struct Neuron
#     input::Array{Float32, 2}
#     active_state_matrix::Array{Float32, 2}

#     output::Array{Float32, 2}

#     function Neuron(input_dim, num_input_nodes, input_seq_len)
#         k = rand(num_input_nodes, input_dim)
#         asm = zeros(input_seq_len, num_input_nodes)
#         return new(k, asm)
#     end
# end

# mutable struct Signal
#     positions::Array{Float32, 2}
#     strengths::Array{Float32}
# end

# mutable struct Layer
#     neurons::Array{Neuron}
#     guiding_keys::Union{Nothing, Dict{Any, Array{Float32, 2}}}

#     function Layer(input_dim, output_dim, num_input_nodes, input_seq_len, output_seq_len, guiding_keys)
#         neurons = [Neuron(input_dim, num_input_nodes, input_seq_len) for _ in 1:output_seq_len]
#         out_p = rand(output_seq_len, output_dim)
#         return new(neurons, guiding_keys, out_p)
#     end
# end



# # # -------------------------------------------------------------

# # mutable struct Node
# #     position::Array{Float32}
# #     # strength::Float32
# # end

# # mutable struct Neuron
# #     input_nodes::Array{Float32, 2}
# #     weight::Float32
# #     output_node::Array{Float32}
# # end

# # # function activate(n::Node, x; activation_range = 1.)
# # #     return rbfk(n.position, x, activation_range)
# # # end

# # function activate(N::Neuron, x; activation_range = 1.)

# #     for n in size(N.input_nodes, 1)
# #         for
# #     I = [activate(n, x, activation_range = activation_range) for n in N.input_nodes]
# #     P = mean(I) * w
# #     return (N.output_node, P)
# # end



# # -------------------------------------------------------------

# function activate!(n::Neuron, x::Signal; activation_range = 1.)
#     for i in 1:size(n.connectivity_matrix, 1)
#         for j in 1:size(n.connectivity_matrix, 2)
#             propg_value = x.strengths[i] * rbfk(x.positions[i, :], n.input_nodes[j, :], activation_range)
#             n.connectivity_matrix[i, j] = propg_value
#         end
#     end
#     return mean(n.connectivity_matrix)
# end

# function activate!(l::Layer, x; activation_range = 1.)
#     activations = [activate!(l.neurons[i], x; activation_range = activation_range) for i in 1:length(l.neurons)]
#     return Signal(l.output_positions, activations)
# end



# # -------------------------------------------------------------

# input_seq_len = 10
# output_seq_len = 6
# input_dim = 3
# output_dim = 2
# num_keys = 4

# l = Layer(input_dim, output_dim, num_keys, input_seq_len, output_seq_len, nothing)
# x = rand(input_seq_len, input_dim)


# activate!(l, x)









# # yf_db = SQLite.DB("C:\\Users\\noone\\Documents\\_PROGRAMMING\\predicting-future\\data\\yahoo_finance.db")
# # (yf_db, "hub_instrument")




# function process(
#     symbol_general_info::SymbolGeneralInfo,
#     symbol_daily_info::SymbolDailyInfo,
#     symbol_hourly_info::SymbolHourlyInfo,

#     symbol_quaterly_update::AbstractArray{SymbolQuaterlyUpdate},
#     symbol_daily_update::AbstractArray{SymbolDailyUpdate},
#     symbol_hourly_update::AbstractArray{SymbolHourlyUpdate},
#     symbol_delta_update::AbstractArray{NonTickDeltaUpdate})


#     z = [
#         encode(symbol_quaterly_update),
#         encode(symbol_daily_update),
#         encode(symbol_hourly_update),
#         encode(symbol_delta_update)]


#     # online test (partial to full online optimization)
#     rec_sym_info_past_days = decode_a(z) # minimal online optimization (must be pre-trained)
#     rec_info_today = decode_b(z) # medium online optimization (also pre-trained)
#     rec_delta_update = decode_c(z) # online optimized (with pre-trained prior, or even multi agent ! )

#     # "informed-prior" test (no online learning, only pre-trained)
#     rec_update_h = decode_d(z) # compared to sym_update_hourly[current_hour]
#     rec_update_d = decode_e(z) # compared to sym_update_daily[today]


#     pred = decode_f(z)
#     # contains:
#     # - daily pivots
#     # - end of day change
#     # - trends and sharp changes (should align with pivots)
#     # - plateous
#     # - volume evolution over day (hourly)
#     # - variance evolution over day (hourly)
#     # - news keywords (hourly) ! for later !
#     # - candels [weighted lower because of high noise to signal ration]
#     # - buy when, sell when (to determine accuracy and consistency outside of prediction error)

#     # then, outside the function,
#     # check all predictions against each other
#     # and check all reconstructions
#     # to evaluate a good trading opportulity

#     # REMEMBER TO SELF: this is not to extract
#     # relevant meta information and making predictions
#     # that adhere to mean regression
#     # but an attempt to directly predict chaos.

# # v1



#     # TODO: how to incorporate update info as priors


#     # u_d = encoder_ud(sym_update_daily)
#     i_d_z = encoder_id(sym_info_past_days)

#     # u_h = encoder_uh(sym_update_hourly)
#     i_h_z = encoder_ih(sym_info_today)


#     # "likelyhood"
#     # are previous days common for current day info
#     likelyhood = decoder_d(i_h_z)
#     # TODO: general likelyhood check by performing croped reconstruction cross entropy testing


#     # "prior"
#     prior = decoder_h(i_d_z)


#     # "posterior"
#     h_from_d_and_h = decoder_dh_h(u_h, i_h, u_d, i_d)


#     # likely given previous days ? (info_past_days)
#     # probable for hours that occured so far ? ()


#     next_hour

# end
